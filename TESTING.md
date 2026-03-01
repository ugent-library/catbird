# Catbird Testing Guide

## Quick Start

```bash
# Start Docker PostgreSQL (one-time setup)
docker compose up -d

# Run all tests (fully isolated, no env vars needed)
./scripts/test.sh

# Run specific test
./scripts/test.sh -run TestQueueCreate

# Stop Docker
docker compose down

# Full reset with volume deletion
docker compose down -v && docker compose up -d
```

## Architecture

The test infrastructure is fully isolated from your local shell environment:

**Components**:
- [docker-compose.yml](docker-compose.yml): PostgreSQL 16 in a container
- [scripts/test.sh](scripts/test.sh): Test runner script (no env vars required)
- [catbird_test.go](catbird_test.go): Test harness with hardcoded connection string

**Key Features**:
- ✅ Hardcoded connection string: `postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable`
- ✅ No environment variables needed
- ✅ Fresh database for each test run (automatic reset)
- ✅ Docker-based isolation
- ✅ Previous runs cannot interfere

## How It Works

1. `docker compose up -d` starts PostgreSQL in a container with persistent volumes
2. `./scripts/test.sh` performs these steps:
   - Checks Docker is running
   - Waits for PostgreSQL to be ready
   - **Drops** the test database (`cb_tst`)
   - **Creates** fresh test database
   - Runs `go test` with hardcoded connection
   - Migrations auto-run via `getTestClient()`:
     - `MigrateDownTo(0)` - Remove all migrations
     - `MigrateUpTo(SchemaVersion)` - Apply fresh schema
     - `verifyMigrationsApplied()` - Verify schema correctness

## Database Reset

The test database is automatically reset before each test run, ensuring:
- No stale data from previous tests
- Fresh schema on every run
- Complete isolation between test sequences

Manual reset:
```bash
# Reset database without stopping Docker
docker compose exec postgres dropdb -U postgres cb_tst
docker compose exec postgres createdb -U postgres cb_tst

# Or use the test script directly
./scripts/test.sh
```

## Connection Details

**Docker Compose**:
- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`
- Database: `cb_tst` (created fresh by test.sh)
- Connection string: `postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable`

These are hardcoded in:
- [catbird_test.go](catbird_test.go#L18): `const testDSN`
- [scripts/test.sh](scripts/test.sh#L13-16): Connection variables

## Test Commands

### All Tests
```bash
./scripts/test.sh
```

### Specific Test
```bash
./scripts/test.sh -run TestBindExactTopic
```

### Multiple Tests
```bash
./scripts/test.sh -run "TestBind.*"
```

### With Verbose Output
```bash
./scripts/test.sh -v
```

### With Coverage
```bash
./scripts/test.sh -cover
```

## Benchmarks

Throughput benchmarks are available for queues, tasks, and flows:
- `BenchmarkQueueThroughput`
- `BenchmarkTaskThroughput`
- `BenchmarkFlowThroughput`

Steady-state pipelined variants (batch submit + batch wait):
- `BenchmarkQueueThroughputBatched`
- `BenchmarkTaskThroughputPipelined`
- `BenchmarkFlowThroughputPipelined`

Tuned high-throughput variants (higher handler concurrency + batch size):
- `BenchmarkTaskThroughputPipelinedTuned`
- `BenchmarkFlowThroughputPipelinedTuned`

These benchmarks use the same Docker PostgreSQL setup and test harness (`getTestClient()`) as regular tests.

### Quick Benchmark Smoke Check
Runs each benchmark once to validate setup without a long run:

```bash
go test -run '^$' -bench 'Benchmark(QueueThroughput|TaskThroughput|FlowThroughput)$' -benchtime=1x .
```

To include pipelined variants:

```bash
go test -run '^$' -bench 'Benchmark(QueueThroughput|TaskThroughput|FlowThroughput|QueueThroughputBatched|TaskThroughputPipelined|FlowThroughputPipelined)$' -benchtime=1x .
```

### Practical Throughput Run
Run for a fixed duration per benchmark and include memory stats:

```bash
go test -run '^$' -bench 'Benchmark(QueueThroughput|TaskThroughput|FlowThroughput)$' -benchmem -benchtime=10s .
```

Pipelined-only steady-state run:

```bash
go test -run '^$' -bench 'Benchmark(TaskThroughputPipelined|FlowThroughputPipelined)$' -benchmem -benchtime=10s .
```

Tuned high-throughput run:

```bash
go test -run '^$' -bench 'Benchmark(TaskThroughputPipelinedTuned|FlowThroughputPipelinedTuned)$' -benchmem -benchtime=10s .
```

Queue batched run:

```bash
go test -run '^$' -bench 'BenchmarkQueueThroughputBatched$' -benchmem -benchtime=10s .
```

### Compare CPU Scaling
Run the same benchmark suite across different `GOMAXPROCS` values:

```bash
go test -run '^$' -bench 'Benchmark(QueueThroughput|TaskThroughput|FlowThroughput)$' -benchmem -benchtime=10s -cpu=1,2,4 .
```

### Include Long-Running Concurrency Tests
Some stress/concurrency tests are optional by default to keep local runs fast.

```bash
CB_SLOW_TESTS=1 ./scripts/test.sh
```

By default, these tests are skipped unless `CB_SLOW_TESTS` is set to `1`, `true`, or `yes`.

Run the map-step concurrency stress test explicitly:
```bash
CB_SLOW_TESTS=1 go test -race ./... -run 'TestFlowMapStepConcurrentWorkersSlow'
```

## Verifying Setup

```bash
# 1. Check Docker is running
docker compose ps
# Output should show: catbird-postgres ... Up

# 2. Connect to database
psql -U postgres -h localhost -d cb_tst
# Should connect successfully
\dt  # List tables
\df cb_*  # List catbird functions

# 3. Run a test
./scripts/test.sh -run TestQueueCreate
```

## Troubleshooting

### "Connection refused"
```bash
# Docker not running?
docker compose up -d

# Or check container status
docker compose ps
```

### "Database doesn't exist"
The database is created automatically by `./scripts/test.sh`. Just run it:
```bash
./scripts/test.sh
```

### "Stale data in tests"
By design, the database is dropped and recreated before each test run:
```bash
./scripts/test.sh -run TestName
```

### "Previous test data interferes"
This shouldn't happen because:
1. Each `./scripts/test.sh` run drops the entire database
2. Creates a completely fresh database
3. Runs migrations from scratch

If you see leftover data, verify:
```bash
docker compose down -v && docker compose up -d
./scripts/test.sh
```

### "PostgreSQL not ready"
The test script waits up to 30 seconds for PostgreSQL to be ready. If it times out:
```bash
# Check logs
docker compose logs postgres

# Restart PostgreSQL
docker compose down
docker compose up -d
```

## CI/CD Integration

For automated testing (GitHub Actions, etc.):

```yaml
- name: Start PostgreSQL
  run: docker compose up -d

- name: Wait for PostgreSQL
  run: for i in {1..30}; do pg_isready -h localhost && break; sleep 1; done

- name: Run tests
  run: ./scripts/test.sh
```

## Migrations & Goose

Migrations are automatically applied during test setup via `getTestClient()`:

**Migration Process**:
1. Read embedded migrations from `migrations/*.sql`
2. Use goose with version tracking table `cb_goose_db_version`
3. Each migration file has `-- +goose up` / `-- +goose down` markers
4. Multi-statement SQL wrapped in `-- +goose statementbegin/end`

**Adding New Migrations**:
1. Create `migrations/00NNN_description.sql`
2. Include `-- +goose up` / `-- +goose down` sections
3. Wrap multi-line statements:
   ```sql
   -- +goose statementbegin
   CREATE OR REPLACE FUNCTION ...
   ... multi-line function ...
   END;
   $$ LANGUAGE plpgsql;
   -- +goose statementend
   ```
4. Update `SchemaVersion` in [migrate.go](migrate.go#L12)

## Files

- [docker-compose.yml](docker-compose.yml): Docker PostgreSQL setup
- [scripts/test.sh](scripts/test.sh): Test runner script
- [catbird_test.go](catbird_test.go): Test harness with `getTestClient()`
- [migrations/](migrations/): SQL migration files (goose-managed)
- [migrate.go](migrate.go): Migration driver code

## See Also

- [Copilot Instructions](.github/copilot-instructions.md) - Full development guide
- [README.md](README.md) - Architecture overview
