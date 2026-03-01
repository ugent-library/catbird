package catbird

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	"github.com/pressly/goose/v3"
)

const SchemaVersion = 13
const gooseVersionTable = "cb_goose_db_version"

//go:embed migrations/*.sql
var migrationsFS embed.FS

func newMigrationProvider(db *sql.DB) (*goose.Provider, error) {
	fs, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}

	p, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		fs,
		goose.WithDisableGlobalRegistry(true),
		goose.WithTableName(gooseVersionTable),
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func MigrateUpTo(ctx context.Context, db *sql.DB, version int) error {
	p, err := newMigrationProvider(db)
	if err != nil {
		return err
	}
	if _, err := p.UpTo(ctx, int64(version)); err != nil {
		return err
	}
	return nil
}

func MigrateDownTo(ctx context.Context, db *sql.DB, version int) error {
	p, err := newMigrationProvider(db)
	if err != nil {
		return err
	}
	if _, err := p.DownTo(ctx, int64(version)); err != nil {
		return err
	}
	return nil
}
