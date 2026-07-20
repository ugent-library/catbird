package wire

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
	var found bool
	if err := db.QueryRowContext(t.Context(), "SELECT cb_wire_mark_read('nobody', 0)").Scan(&found); err != nil {
		t.Fatalf("cb_wire_mark_read: %v", err)
	}
	if found {
		t.Fatal("cb_wire_mark_read('nobody', 0) = true, want false")
	}

	// Down leaves no module objects behind — no tables, sequences or
	// functions. Only goose's version table and the kernel's SQL unit
	// stay. The query is scoped to this module's namespace: the streams
	// and jobs suites run against the same database in parallel, and
	// cb_wire_nodes/cb_wire_presence are the old root schema's tables,
	// live in the same database until the old schema is dropped.
	if err := MigrateDownTo(t.Context(), db, 0); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	var leftover int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM (
		SELECT relname FROM pg_class
		WHERE relname LIKE 'cb\_wire%'
		  AND relname NOT LIKE 'cb\_wire\_migrations%'
		  AND relname NOT IN ('cb_wire_nodes', 'cb_wire_presence')
		  AND relkind IN ('r', 'p', 'S')
		UNION ALL
		SELECT proname FROM pg_proc
		WHERE proname LIKE '%cb\_wire\_%' AND pronamespace = 'public'::regnamespace) x`).Scan(&leftover); err != nil {
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
