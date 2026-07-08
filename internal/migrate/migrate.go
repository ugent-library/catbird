package migrate

import (
	"context"
	"database/sql"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

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

func UpTo(ctx context.Context, db *sql.DB, fsys fs.FS, table string, version int) error {
	p, err := newProvider(db, fsys, table)
	if err != nil {
		return err
	}
	_, err = p.UpTo(ctx, int64(version))
	return err
}

func DownTo(ctx context.Context, db *sql.DB, fsys fs.FS, table string, version int) error {
	p, err := newProvider(db, fsys, table)
	if err != nil {
		return err
	}
	_, err = p.DownTo(ctx, int64(version))
	return err
}
