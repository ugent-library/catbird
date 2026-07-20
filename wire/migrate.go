package wire

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	"github.com/ugent-library/catbird/internal/migrate"
)

const SchemaVersion = 1
const migrationsTable = "cb_wire_migrations"

//go:embed migrations/*.sql
var migrationsFS embed.FS

func MigrateUpTo(ctx context.Context, db *sql.DB, version int) error {
	fsys, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	return migrate.UpTo(ctx, db, fsys, migrationsTable, version)
}

func MigrateDownTo(ctx context.Context, db *sql.DB, version int) error {
	fsys, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	return migrate.DownTo(ctx, db, fsys, migrationsTable, version)
}
