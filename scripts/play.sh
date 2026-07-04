#!/usr/bin/env bash
# Rebuild the cb_scratch database from migrations/00006_stream.sql and open psql.
# Edit the migration, rerun this, play again in a fresh world.
set -euo pipefail
cd "$(dirname "$0")/.."

docker exec catbird-postgres psql -U postgres -q \
  -c "DROP DATABASE IF EXISTS cb_scratch;" \
  -c "CREATE DATABASE cb_scratch;"

sed -n '/+goose up/,/+goose down/p' migrations/00006_stream.sql \
  | grep -v '\-\- +goose down' \
  | docker exec -i catbird-postgres psql -U postgres -d cb_scratch -q -v ON_ERROR_STOP=1

echo "cb_scratch rebuilt from 00006_stream.sql"
exec docker exec -it catbird-postgres psql -U postgres -d cb_scratch
