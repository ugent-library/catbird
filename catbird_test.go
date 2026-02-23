package catbird

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var testClient *Client
var testPool *pgxpool.Pool
var testOnce sync.Once

func TestMain(m *testing.M) {
	code := m.Run()
	if testPool != nil {
		testPool.Close()
	}
	os.Exit(code)
}

func startTestWorker(t *testing.T, worker *Worker) {
	t.Helper()
	worker.shutdownTimeout = 0

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() {
		errCh <- worker.Start(ctx)
	}()

	// Wait briefly for Start to complete or error
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("worker.Start() failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		// Worker started successfully, continue
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("worker shutdown error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("worker did not shutdown within timeout")
		}
	})
}

// Hardcoded test database connection
// See docker-compose.yml and scripts/test.sh for setup
const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

// verifyMigrationsApplied checks if key functions exist after migration
func verifyMigrationsApplied(ctx context.Context, db *sql.DB) error {
	// Check for functions that should exist at different schema versions
	requiredFunctions := map[int][]string{
		1: {"cb_create_queue", "cb_send", "cb_read"},
		2: {"cb_create_task", "cb_run_task", "cb_create_flow"},
		3: {"cb_gc"},
		4: {"cb_parse_condition", "cb_evaluate_condition", "cb_evaluate_condition_expr"},
	}

	for version, functions := range requiredFunctions {
		if version > SchemaVersion {
			break
		}

		for _, fname := range functions {
			var exists bool
			err := db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1)",
				fname).Scan(&exists)
			if err != nil {
				return fmt.Errorf("error checking function %s: %w", fname, err)
			}
			if !exists {
				return fmt.Errorf("migration failed: function %s does not exist (schema v%d)", fname, version)
			}
		}
	}

	return nil
}

func getTestClient(t *testing.T) *Client {
	testOnce.Do(func() {
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

		logger.Info("test harness", "dsn", testDSN)

		// Open SQL DB for migrations
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			t.Fatalf("error opening database: %v", err)
		}
		defer db.Close()

		// Verify connection
		if err := db.Ping(); err != nil {
			t.Fatalf("error connecting to database: %v\nMake sure Docker is running: docker compose up -d", err)
		}
		logger.Info("connected to test database")

		// Migrate down to clean state (skip if no tables exist)
		logger.Info("starting migration", "step", "down")
		if err := MigrateDownTo(ctx, db, 0); err != nil {
			// Ignore errors on first run when no tables exist
			logger.Warn("migration down encountered error (may be normal on first run)", "error", err)
		}
		logger.Info("migration complete", "step", "down")

		// Migrate up to current schema
		logger.Info("starting migration", "step", "up", "target_version", SchemaVersion)
		if err := MigrateUpTo(ctx, db, SchemaVersion); err != nil {
			t.Fatalf("migration up failed: %v", err)
		}
		logger.Info("migration complete", "step", "up", "version", SchemaVersion)

		// Verify migrations were applied
		if err := verifyMigrationsApplied(ctx, db); err != nil {
			t.Fatalf("migration verification failed: %v", err)
		}
		logger.Info("migration verification passed")

		// Create connection pool
		pool, err := pgxpool.New(ctx, testDSN)
		if err != nil {
			t.Fatalf("error creating connection pool: %v", err)
		}
		testPool = pool
		testClient = New(pool)
		logger.Info("test client initialized")
	})

	return testClient
}
