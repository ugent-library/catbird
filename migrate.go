package catbird

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationsEmbedFS embed.FS

// MigrationsFS is the migration files as they sit on disk, one
// <number>_<name>.up.sql and one <number>_<name>.down.sql per version.
var MigrationsFS fs.FS = func() fs.FS {
	sub, err := fs.Sub(migrationsEmbedFS, "migrations")
	if err != nil {
		panic(err)
	}
	return sub
}()

// Migration is one schema change, parsed. A caller who runs a migration tool
// of their own registers UpSQL and DownSQL with it instead of calling
// MigrateUp; each is a plain script the tool executes whole.
type Migration struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

// Migrations returns every migration, sorted by version. Every version has
// both files: a down that drops what its up created is what MigrateDownTo
// runs, and a version missing one would only be found when a rollback needs
// it.
func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(MigrationsFS, ".")
	if err != nil {
		return nil, fmt.Errorf("catbird: read migrations: %w", err)
	}
	byVersion := map[int64]*migrationFiles{}
	for _, e := range entries {
		if err := parseMigrationFile(e.Name(), byVersion); err != nil {
			return nil, err
		}
	}
	var migrations []Migration
	for _, f := range byVersion {
		if !f.hasUp {
			return nil, fmt.Errorf("catbird: migration %d %s: no up file", f.Version, f.Name)
		}
		if !f.hasDown {
			return nil, fmt.Errorf("catbird: migration %d %s: no down file", f.Version, f.Name)
		}
		migrations = append(migrations, f.Migration)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, nil
}

// migrationFiles is a migration while its files are being read, with which of
// the two have been seen.
type migrationFiles struct {
	Migration
	hasUp, hasDown bool
}

// parseMigrationFile reads one file into the migration of its version,
// creating it on the first file seen.
func parseMigrationFile(filename string, byVersion map[int64]*migrationFiles) error {
	malformed := fmt.Errorf("catbird: migration %s: file name is not <number>_<name>.up.sql or <number>_<name>.down.sql", filename)
	stem, up := strings.CutSuffix(filename, ".up.sql")
	if !up {
		var down bool
		if stem, down = strings.CutSuffix(filename, ".down.sql"); !down {
			return malformed
		}
	}
	number, name, ok := strings.Cut(stem, "_")
	if !ok {
		return malformed
	}
	version, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return malformed
	}
	b, err := fs.ReadFile(MigrationsFS, filename)
	if err != nil {
		return fmt.Errorf("catbird: migration %s: %w", filename, err)
	}
	f, seen := byVersion[version]
	if !seen {
		f = &migrationFiles{Migration: Migration{Version: version, Name: name}}
		byVersion[version] = f
	} else if f.Name != name {
		return fmt.Errorf("catbird: migrations %d_%s and %s have the same version", version, f.Name, filename)
	}
	if up {
		f.UpSQL, f.hasUp = string(b), true
	} else {
		f.DownSQL, f.hasDown = string(b), true
	}
	return nil
}

// TxBeginner is what the runner needs that Conn does not have: each migration
// runs in its own transaction, and BEGIN as a plain statement on a pool may
// land on a different connection than the statements after it. *pgx.Conn,
// *pgxpool.Pool and pgx.Tx all satisfy it.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// cb_migrations records which migrations ran. The runner creates it on first
// use, so no migration has to; it is written once per schema change and read
// by nothing on a hot path.
const createMigrationsTableSQL = `
    CREATE TABLE IF NOT EXISTS cb_migrations (
        version BIGINT PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`

// MigrateUp applies every migration not yet applied, in version order, each
// in its own transaction together with its cb_migrations row. Two processes
// deploying at once queue on an advisory lock, and the one that waited sees
// the row and skips. Everything runs in a transaction, so a migration cannot
// use CREATE INDEX CONCURRENTLY until the runner grows a marker for it.
func MigrateUp(ctx context.Context, db TxBeginner) error {
	migrations, err := Migrations()
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if err := migrateStep(ctx, db, m.UpSQL,
			`INSERT INTO cb_migrations (version, name) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
			m.Version, m.Name); err != nil {
			return fmt.Errorf("catbird: migration %d %s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// MigrateDownTo reverts every applied migration above version, newest first.
// MigrateDownTo(ctx, db, 0) reverts everything.
func MigrateDownTo(ctx context.Context, db TxBeginner, version int64) error {
	migrations, err := Migrations()
	if err != nil {
		return err
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]
		if m.Version <= version {
			break
		}
		if err := migrateStep(ctx, db, m.DownSQL,
			`DELETE FROM cb_migrations WHERE version = $1`,
			m.Version); err != nil {
			return fmt.Errorf("catbird: migration %d %s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}

// migrateStep runs one migration in one transaction: take the lock, record
// the step, and only when the record changed a row — nobody did it first —
// run its SQL. Lock 3 under catbird's namespace; the assigner's transaction
// lock is 1 and the test binaries' session lock is 2.
func migrateStep(ctx context.Context, db TxBeginner, sql, record string, args ...any) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('catbird'), 3)`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, createMigrationsTableSQL); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, record, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // already applied, or already reverted
	}
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
