#!/usr/bin/env bash
# Torture test for the assigner. The whole design rests on one claim: no
# matter how many publishers run at once, and no matter how long one of them
# holds its transaction open, every committed message gets exactly one
# position and the positions count up from 1 without holes.
#
# This script attacks that claim: many concurrent publishers, some of them
# committing only after a long pause, while two assigners compete the whole
# time. Afterwards it checks that nothing was lost, nothing was numbered
# twice, and a cursor can read every message back.
set -euo pipefail
cd "$(dirname "$0")/.."

DB=cb_scratch
PSQL=(docker exec -i catbird-postgres psql -U postgres -q -v ON_ERROR_STOP=1 -d "$DB")
DURATION="${1:-8}"   # seconds of pgbench load; pass a bigger number for a longer soak

docker exec catbird-postgres psql -U postgres -q \
  -c "DROP DATABASE IF EXISTS $DB;" -c "CREATE DATABASE $DB;"
sed -n '/+goose up/,/+goose down/p' stream/migrations/00001_stream.sql \
  | grep -v '\-\- +goose down' | "${PSQL[@]}"
"${PSQL[@]}" -c "SELECT cb_stream_ensure('torture');"

# pgbench script: publish once per transaction; ~5% of transactions stay open
# for 200ms — long enough to straddle several assigner ticks, the exact case
# the 50ms-watermark design lost messages on.
docker exec -i catbird-postgres sh -c 'cat > /tmp/cb_publish.sql' <<'EOF'
\set slow random(1, 100)
BEGIN;
SELECT p.ref_id FROM cb_stream_publish('torture', 't', '1') p;
\if :slow <= 5
SELECT pg_sleep(0.2);
\endif
COMMIT;
EOF

# two competing assigners, ticking as fast as they can for the whole run
assigner() {
  while [ -e /tmp/cb_torture_running ]; do
    docker exec catbird-postgres psql -U postgres -q -d "$DB" -tA \
      -c "SELECT _cb_stream_assign_positions('torture', 1000);" >/dev/null 2>&1 || true
    sleep 0.05
  done
}
trap 'rm -f /tmp/cb_torture_running' EXIT
touch /tmp/cb_torture_running
assigner & A1=$!
assigner & A2=$!

echo "== pgbench: 12 clients, ${DURATION}s, ~5% slow transactions =="
docker exec catbird-postgres pgbench -U postgres -d "$DB" -n \
  -c 12 -j 4 -T "$DURATION" -f /tmp/cb_publish.sql | grep -E "tps|number of transactions"

rm -f /tmp/cb_torture_running
wait "$A1" "$A2" 2>/dev/null || true

# drain: everything published must end up assigned
while [ "$(docker exec catbird-postgres psql -U postgres -d "$DB" -tA \
  -c "SELECT _cb_stream_assign_positions('torture', 10000);")" != "0" ]; do :; done

echo "== invariants =="
"${PSQL[@]}" <<'EOF'
DO $$
DECLARE
    _total bigint; _assigned bigint; _distinct bigint;
    _min bigint; _max bigint; _last bigint;
BEGIN
    SELECT count(*), count(m.pos), count(DISTINCT m.pos),
           coalesce(min(m.pos), 0), coalesce(max(m.pos), 0)
    INTO _total, _assigned, _distinct, _min, _max
    FROM cb_stream_messages m WHERE m.stream = 'torture';
    SELECT s.last_pos INTO _last FROM cb_streams s WHERE s.name = 'torture';

    ASSERT _total > 0, 'nothing was published';
    ASSERT _assigned = _total,
        format('%s of %s messages never got a position (lost)', _total - _assigned, _total);
    ASSERT _distinct = _total, 'duplicate positions (double-stamping)';
    ASSERT _min = 1 AND _max = _total,
        format('positions are not dense 1..%s (min %s, max %s)', _total, _min, _max);
    ASSERT _last = _total,
        format('last_pos %s does not match message count %s (gap)', _last, _total);
    RAISE NOTICE 'OK: % messages, positions dense 1..%, last_pos matches', _total, _max;
END $$;

-- a cursor can read every message back, in order, nothing missing
SELECT cb_stream_ensure_cursor('torture', 'audit', 0);
DO $$
DECLARE _read bigint := 0; _batch bigint; _last bigint;
BEGIN
    SELECT s.last_pos INTO _last FROM cb_streams s WHERE s.name = 'torture';
    LOOP
        SELECT count(*) INTO _batch FROM cb_stream_read('torture', 'audit', 5000);
        EXIT WHEN _batch = 0;
        _read := _read + _batch;
    END LOOP;
    ASSERT _read = _last, format('cursor read %s of %s messages', _read, _last);
    RAISE NOTICE 'OK: cursor read all % messages', _read;
END $$;
EOF
echo "TORTURE TEST PASSED"
