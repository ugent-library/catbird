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

// MigrationsFS is the migration files as they sit on disk:
// <number>_<name>.sql with -- +goose up and -- +goose down markers, which
// goose reads as they are. A caller who runs goose points a provider with its
// own table name at this instead of calling MigrateUp.
var MigrationsFS fs.FS = func() fs.FS {
	sub, err := fs.Sub(migrationsEmbedFS, "migrations")
	if err != nil {
		panic(err)
	}
	return sub
}()

// Migration is one schema change, parsed. A caller who runs a migration tool
// of their own registers UpSQL and DownSQL with it instead of calling
// MigrateUp; because the schema has no PL/pgSQL, each is plain statements any
// tool can execute as one script.
type Migration struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

// Migrations returns every migration, sorted by version.
func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(MigrationsFS, ".")
	if err != nil {
		return nil, fmt.Errorf("catbird: read migrations: %w", err)
	}
	var migrations []Migration
	for _, e := range entries {
		m, err := parseMigration(e.Name())
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, m)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version == migrations[i-1].Version {
			return nil, fmt.Errorf("catbird: migrations %s and %s have the same version", migrations[i-1].Name, migrations[i].Name)
		}
	}
	return migrations, nil
}

func parseMigration(filename string) (Migration, error) {
	number, name, ok := strings.Cut(strings.TrimSuffix(filename, ".sql"), "_")
	if !ok {
		return Migration{}, fmt.Errorf("catbird: migration %s: file name is not <number>_<name>.sql", filename)
	}
	version, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return Migration{}, fmt.Errorf("catbird: migration %s: file name is not <number>_<name>.sql", filename)
	}
	b, err := fs.ReadFile(MigrationsFS, filename)
	if err != nil {
		return Migration{}, fmt.Errorf("catbird: migration %s: %w", filename, err)
	}
	// The schema has no PL/pgSQL, so no statement holds a semicolon inside a
	// body and each section runs as one script; the markers are all a parser
	// needs. Both are required: every migration's down drops what its up
	// created.
	_, rest, ok := strings.Cut(string(b), "-- +goose up")
	if !ok {
		return Migration{}, fmt.Errorf("catbird: migration %s: no -- +goose up marker", filename)
	}
	up, down, ok := strings.Cut(rest, "-- +goose down")
	if !ok {
		return Migration{}, fmt.Errorf("catbird: migration %s: no -- +goose down marker", filename)
	}
	return Migration{Version: version, Name: name, UpSQL: up, DownSQL: down}, nil
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
