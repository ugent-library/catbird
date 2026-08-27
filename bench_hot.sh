#!/bin/bash

# Index Bloat Benchmark for Catbird - Bottom-Up Index Deletion Test
# Tests whether Postgres 14+ BUID handles visible_at index churn at medium load
# Run against: postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable

set -e

DB_URL="postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"
TABLE_NAME="cb_claims_test"
INDEX_NAME="idx_claims_queue_visible"

# We want enough rows and churn to force Postgres to allocate new B-Tree pages
# if it is unable to recycle dead tuples internally.
INITIAL_ROWS=10000
CHURN_ITERATIONS=200
UPDATES_PER_ITERATION=2000
MEASURE_INTERVAL=20

log_info() {
    echo -e "\033[0;34m[INFO]\033[0m $1"
}

measure_index_size() {
    psql "$DB_URL" -t -c "SELECT pg_relation_size('$INDEX_NAME'::regclass);" | tr -d ' '
}

measure_table_size() {
    psql "$DB_URL" -t -c "SELECT pg_total_relation_size('$TABLE_NAME'::regclass);" | tr -d ' '
}

cleanup() {
    psql "$DB_URL" -c "DROP TABLE IF EXISTS $TABLE_NAME CASCADE;" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# === SETUP PHASE ===
log_info "Setting up test table and index..."

psql "$DB_URL" << 'SQL' >/dev/null
DROP TABLE IF EXISTS cb_claims_test CASCADE;

CREATE TABLE cb_claims_test (
    id BIGSERIAL PRIMARY KEY,
    queue TEXT NOT NULL,
    visible_at TIMESTAMP NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0,
    payload JSONB DEFAULT '{}'::jsonb
);

-- The Catbird Lite target index
CREATE INDEX idx_claims_queue_visible ON cb_claims_test (queue, visible_at) WHERE status = 0;
SQL

# === DATA LOAD PHASE ===
log_info "Inserting $INITIAL_ROWS initial rows..."
psql "$DB_URL" << SQL >/dev/null
INSERT INTO cb_claims_test (queue, visible_at, status, payload)
SELECT
    'default',
    NOW() + (random() * interval '60 seconds'),
    0,
    '{"data": "test"}'::jsonb
FROM generate_series(1, $INITIAL_ROWS);
SQL

# === MEASUREMENT PHASE ===
log_info "Starting index churn benchmark. Iterations: $CHURN_ITERATIONS, Chunk: $UPDATES_PER_ITERATION"
echo ""
printf "%-12s %-15s %-15s\n" "Iteration" "Index (Bytes)" "Table (Bytes)"
printf "%-12s %-15s %-15s\n" "-----------" "---------------" "---------------"

start_idx_size=$(measure_index_size)
iter=0
printf "%-12d %-15s %-15s\n" "$iter" "$start_idx_size" "$(measure_table_size)"

for iter in $(seq 1 $CHURN_ITERATIONS); do
    # 1. Lease rows (status 0 -> 1)
    psql "$DB_URL" -q << SQL
    UPDATE cb_claims_test
    SET status = 1, visible_at = NOW() + interval '5 minutes'
    WHERE id IN (
        SELECT id FROM cb_claims_test WHERE status = 0 LIMIT $UPDATES_PER_ITERATION
    );
SQL

    # 2. Fail and backoff (status 1 -> 0, advance visible_at)
    psql "$DB_URL" -q << SQL
    UPDATE cb_claims_test
    SET status = 0, visible_at = NOW() + (random() * interval '30 seconds')
    WHERE status = 1;
SQL

    if [ $((iter % MEASURE_INTERVAL)) -eq 0 ]; then
        idx_size=$(measure_index_size)
        tbl_size=$(measure_table_size)
        printf "%-12d %-15s %-15s\n" "$iter" "$idx_size" "$tbl_size"
    fi
done

final_idx_size=$(measure_index_size)
idx_growth=$((final_idx_size - start_idx_size))

echo ""
log_info "Total Updates Executed: $((CHURN_ITERATIONS * UPDATES_PER_ITERATION * 2))"
log_info "Final Index Growth: $idx_growth bytes"
