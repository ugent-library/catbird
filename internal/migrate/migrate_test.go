package migrate

import (
	"database/sql"
	"testing"
	"testing/fstest"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

// A minimal module: UpTo must apply the kernel unit before it, and DownTo
// must leave the kernel unit in place.
var noopModuleFS = fstest.MapFS{
	"00001_noop.sql": &fstest.MapFile{Data: []byte(
		"-- +goose up\nSELECT 1;\n-- +goose down\nSELECT 1;\n")},
}

const noopModuleTable = "migrate_tst_migrations"

func TestKernelUnit(t *testing.T) {
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := UpTo(t.Context(), db, noopModuleFS, noopModuleTable, 1); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	// applying again is a no-op
	if err := UpTo(t.Context(), db, noopModuleFS, noopModuleTable, 1); err != nil {
		t.Fatalf("migrate up twice: %v", err)
	}

	var valid bool
	for name, want := range map[string]bool{
		"orders":                true,
		"a1_b2":                 true,
		"Orders":                false,
		"1orders":               false,
		"a.b":                   false,
		"a_name_of_twenty_one_": false,
	} {
		if err := db.QueryRowContext(t.Context(),
			"SELECT cb_valid_name($1)", name).Scan(&valid); err != nil {
			t.Fatalf("cb_valid_name(%q): %v", name, err)
		}
		if valid != want {
			t.Errorf("cb_valid_name(%q) = %v, want %v", name, valid, want)
		}
	}

	var forever string
	if err := db.QueryRowContext(t.Context(), "SELECT cb_forever()::text").Scan(&forever); err != nil {
		t.Fatalf("cb_forever: %v", err)
	}
	if forever != "-00:00:01" {
		t.Fatalf("cb_forever() = %q", forever)
	}

	var secs float64
	if err := db.QueryRowContext(t.Context(),
		"SELECT extract(epoch FROM cb_backoff('none', '1s', '1m', 3))::float8").Scan(&secs); err != nil {
		t.Fatal(err)
	}
	if secs != 0 {
		t.Errorf("cb_backoff(none) = %vs, want 0", secs)
	}
	if err := db.QueryRowContext(t.Context(),
		"SELECT extract(epoch FROM cb_backoff('fixed', '5s', '2s', 1))::float8").Scan(&secs); err != nil {
		t.Fatal(err)
	}
	if secs != 2 {
		t.Errorf("cb_backoff(fixed, 5s, 2s) = %vs, want 2s (capped)", secs)
	}
	// full jitter at attempt 4: uniform in [0, min(1s * 2^3, 1m)] = [0, 8s]
	for range 20 {
		if err := db.QueryRowContext(t.Context(),
			"SELECT extract(epoch FROM cb_backoff('full_jitter', '1s', '1m', 4))::float8").Scan(&secs); err != nil {
			t.Fatal(err)
		}
		if secs < 0 || secs > 8 {
			t.Errorf("cb_backoff(full_jitter, 1s, 1m, 4) = %vs, want within [0, 8s]", secs)
		}
	}

	// module down leaves the kernel unit standing
	if err := DownTo(t.Context(), db, noopModuleFS, noopModuleTable, 0); err != nil {
		t.Fatalf("migrate down: %v", err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT cb_forever()::text").Scan(&forever); err != nil {
		t.Fatalf("cb_forever after module down: %v", err)
	}
}
