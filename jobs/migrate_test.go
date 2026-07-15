package jobs

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

	// Prove the schema landed: 'default' is seeded with the stated terms.
	var claimTTL string
	var batch, maxAttempts int
	var kind, base, max string
	if err := db.QueryRowContext(t.Context(),
		`SELECT claim_ttl::text, claim_batch_size, max_attempts,
		        backoff_kind::text, backoff_base::text, backoff_max::text
		 FROM cb_job_queues WHERE name = 'default'`).
		Scan(&claimTTL, &batch, &maxAttempts, &kind, &base, &max); err != nil {
		t.Fatalf("default queue row: %v", err)
	}
	if claimTTL != "00:00:30" || batch != 10 || maxAttempts != 3 ||
		kind != "full_jitter" || base != "00:00:01" || max != "00:01:00" {
		t.Fatalf("default queue terms = (%s, %d, %d, %s, %s, %s)",
			claimTTL, batch, maxAttempts, kind, base, max)
	}

	// Down leaves no module objects behind — no tables, sequences,
	// functions or enum types. Only goose's version table and the kernel's
	// SQL unit stay. The query is scoped to this module's namespace: the
	// stream suite runs against the same database in parallel.
	if err := MigrateDownTo(t.Context(), db, 0); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	var leftover int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM (
		SELECT relname FROM pg_class
		WHERE relname LIKE 'cb\_job%' AND relname NOT LIKE 'cb\_job\_migrations%'
		  AND relkind IN ('r', 'p', 'S')
		UNION ALL
		SELECT proname FROM pg_proc
		WHERE proname LIKE '%cb\_job%' AND pronamespace = 'public'::regnamespace
		UNION ALL
		SELECT typname FROM pg_type
		WHERE typname LIKE 'cb\_job%' AND typtype = 'e') x`).Scan(&leftover); err != nil {
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
