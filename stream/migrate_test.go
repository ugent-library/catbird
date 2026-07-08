package stream

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

func TestMigrate(t *testing.T) {
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := MigrateUpTo(t.Context(), db, SchemaVersion); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	// Prove the schema landed.
	var forever string
	if err := db.QueryRowContext(t.Context(), "SELECT cb_forever()::text").Scan(&forever); err != nil {
		t.Fatalf("cb_forever: %v", err)
	}
	if forever != "-00:00:01" {
		t.Fatalf("cb_forever() = %q", forever)
	}
}
