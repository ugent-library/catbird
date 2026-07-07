#!/usr/bin/env bash
# Rebuild cb_scratch from migrations/00006_stream.sql, run the scenario tests
# in scripts/stream_test.sql, then run the down section and verify it leaves
# no catbird objects behind. Exits non-zero on the first failed assertion.
set -euo pipefail
cd "$(dirname "$0")/.."

PSQL=(docker exec -i catbird-postgres psql -U postgres -q -v ON_ERROR_STOP=1)

docker exec catbird-postgres psql -U postgres -q \
  -c "DROP DATABASE IF EXISTS cb_scratch;" \
  -c "CREATE DATABASE cb_scratch;"

sed -n '/+goose up/,/+goose down/p' migrations/00006_stream.sql \
  | grep -v '\-\- +goose down' \
  | "${PSQL[@]}" -d cb_scratch

"${PSQL[@]}" -d cb_scratch < scripts/stream_test.sql

sed -n '/+goose down/,$p' migrations/00006_stream.sql \
  | grep -v '\-\- +goose down' \
  | "${PSQL[@]}" -d cb_scratch

leftover=$(docker exec catbird-postgres psql -U postgres -d cb_scratch -tA -c "
  SELECT count(*) FROM (
    SELECT relname FROM pg_class WHERE relname LIKE 'cb%' AND relkind IN ('r','p','S')
    UNION ALL
    SELECT proname FROM pg_proc WHERE proname LIKE '%cb\_%' AND pronamespace = 'public'::regnamespace
    UNION ALL
    SELECT typname FROM pg_type WHERE typname LIKE 'cb\_%' AND typtype = 'e') x")

if [ "$leftover" != "0" ]; then
  echo "DOWN LEFT $leftover OBJECTS BEHIND" >&2
  exit 1
fi
echo "down section clean: 0 objects left"
