package migrate

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// The kernel's own SQL unit: shared pure functions every module's schema
// uses. UpTo applies it before the module's migrations, so no caller has a
// separate install step for it.
const kernelTable = "cb_kernel_migrations"

//go:embed migrations/*.sql
var kernelFS embed.FS

// Each module embeds its own migrations directory and tracks its progress
// in its own goose table.
func newProvider(db *sql.DB, fsys fs.FS, table string) (*goose.Provider, error) {
	// go test ./... runs packages as separate processes that migrate the
	// same database at the same time. The session lock prevents running
	// migrations concurrently.
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, err
	}
	return goose.NewProvider(
		goose.DialectPostgres,
		db,
		fsys,
		goose.WithDisableGlobalRegistry(true),
		goose.WithTableName(table),
		goose.WithSessionLocker(locker),
	)
}

func kernelUp(ctx context.Context, db *sql.DB) error {
	fsys, err := fs.Sub(kernelFS, "migrations")
	if err != nil {
		return err
	}
	p, err := newProvider(db, fsys, kernelTable)
	if err != nil {
		return err
	}
	_, err = p.Up(ctx)
	return err
}

func UpTo(ctx context.Context, db *sql.DB, fsys fs.FS, table string, version int) error {
	if err := kernelUp(ctx, db); err != nil {
		return err
	}
	p, err := newProvider(db, fsys, table)
	if err != nil {
		return err
	}
	_, err = p.UpTo(ctx, int64(version))
	return err
}

// DownTo reverts the module's migrations only. The kernel unit stays: other
// modules in the same database still use it.
func DownTo(ctx context.Context, db *sql.DB, fsys fs.FS, table string, version int) error {
	p, err := newProvider(db, fsys, table)
	if err != nil {
		return err
	}
	_, err = p.DownTo(ctx, int64(version))
	return err
}
