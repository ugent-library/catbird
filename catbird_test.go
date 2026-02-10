package catbird

import (
	"database/sql"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var testClient *Client
var testOnce sync.Once

func getTestClient(t *testing.T) *Client {
	testOnce.Do(func() {
		dsn := os.Getenv("CB_CONN")

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		MigrateDownTo(t.Context(), db, 0)
		MigrateUpTo(t.Context(), db, SchemaVersion)

		pool, err := pgxpool.New(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}

		testClient = New(pool)
	})

	return testClient
}
