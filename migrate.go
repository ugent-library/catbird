package catbird

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Migration is one schema change. A caller who runs a migration tool of their
// own executes UpSQL and DownSQL with it instead of calling MigrateUp; each is
// a plain script the tool runs whole.
type Migration struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

// Each migration is two files, <number>_<name>.up.sql and
// <number>_<name>.down.sql, embedded here and listed in migrations below in
// version order.

//go:embed migrations/00001_schema.up.sql
var schemaUpSQL string

//go:embed migrations/00001_schema.down.sql
var schemaDownSQL string

var migrations = []Migration{
	{Version: 1, Name: "schema", UpSQL: schemaUpSQL, DownSQL: schemaDownSQL},
}

// Migrations returns every migration in version order.
func Migrations() []Migration {
	return append([]Migration(nil), migrations...)
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
