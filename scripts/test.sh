#!/bin/bash
set -e

# Catbird Test Runner
# Uses Docker Compose for isolated, reproducible testing
# Hardcoded connection string (no env vars needed)

# Constants
CONTAINER_NAME="catbird-postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_NAME="cb_tst"
CONNECTION_STRING="postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=disable"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
die() {
  echo -e "${RED}ERROR: $*${NC}" >&2
  exit 1
}

info() {
  echo -e "${YELLOW}$*${NC}"
}

success() {
  echo -e "${GREEN}$*${NC}"
}

check_docker() {
  if ! docker ps > /dev/null 2>&1; then
    die "Docker is not running. Please start Docker and try again."
  fi

  if ! docker compose ps 2>/dev/null | grep -q "$CONTAINER_NAME"; then
    die "PostgreSQL container not running. Run 'docker compose up -d' first."
  fi
}

wait_for_postgres() {
  info "Waiting for PostgreSQL to be ready..."
  for i in {1..30}; do
    if pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" > /dev/null 2>&1; then
      success "PostgreSQL is ready!"
      return 0
    fi
    sleep 1
  done
  die "PostgreSQL failed to start after 30 seconds"
}

drop_test_db() {
  info "Dropping test database..."
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" \
    -t -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
}

create_test_db() {
  info "Creating test database..."
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" \
    -t -c "CREATE DATABASE $DB_NAME;" 2>/dev/null || die "Failed to create database"
}

# Main
main() {
  check_docker
  wait_for_postgres
  drop_test_db
  create_test_db

  info "Running tests..."
  info "Connection: $CONNECTION_STRING"

  # Run tests with hardcoded connection
  CB_CONN="$CONNECTION_STRING" go test -v -timeout 30m "$@" ./...

  success "Tests complete!"
}

# Run
main "$@"
