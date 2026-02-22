package catbird_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ugent-library/catbird"
)

func TestForceMigrations(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Migrate down
	fmt.Println("Migrating down...")
	if err := catbird.MigrateDownTo(ctx, db, 0); err != nil {
		fmt.Printf("Migrate down error (may be normal): %v\n", err)
	}

	// Migrate up
	fmt.Println("Migrating up...")
	if err := catbird.MigrateUpTo(ctx, db, catbird.SchemaVersion); err != nil {
		t.Fatal(err)
	}

	fmt.Println("Done!")
}
