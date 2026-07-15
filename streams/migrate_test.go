package streams

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

	// Down leaves no module objects behind — no tables, partitions,
	// sequences, functions or enum types. Only goose's version tables and
	// the kernel's SQL unit (shared with the other modules) stay.
	if err := MigrateDownTo(t.Context(), db, 0); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	var leftover int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM (
		SELECT relname FROM pg_class
		WHERE relname LIKE 'cb%' AND relname NOT LIKE 'cb\_stream\_migrations%'
		  AND relname NOT LIKE 'cb\_kernel\_migrations%'
		  AND relkind IN ('r', 'p', 'S')
		UNION ALL
		SELECT proname FROM pg_proc
		WHERE proname LIKE '%cb\_%' AND pronamespace = 'public'::regnamespace
		  AND proname NOT IN ('cb_valid_name', 'cb_forever', 'cb_backoff')
		UNION ALL
		SELECT typname FROM pg_type
		WHERE typname LIKE 'cb\_%' AND typtype = 'e'
		  AND typname <> 'cb_backoff_kind') x`).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("down left %d objects behind", leftover)
	}

	// the rest of the suite runs on the schema: bring it back
	if err := MigrateUpTo(t.Context(), db, SchemaVersion); err != nil {
		t.Fatalf("migrate up again: %v", err)
	}
}
