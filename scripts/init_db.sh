#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DB_URL:?Set DB_URL, e.g. postgresql://postgres:postgres@localhost:5432/mydb}"

psql "$DB_URL" -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'

# TODO: make a list of scripts [script1, script2, ...] to avoid dependency conflicts
# script 2 may refer to the table in script 1, so script 1 msut run first
for f in ./scripts/*.sql; do
  echo "Running $f"
  psql "$DB_URL" -v ON_ERROR_STOP=1 -f "$f"
done
