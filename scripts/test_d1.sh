#!/bin/bash
set -euo pipefail

# Usage: test_d1.sh <ACCOUNT_ID> <API_TOKEN> <DATABASE_ID>
ACC_ID=${1:-}
API_TOKEN=${2:-}
DB_ID=${3:-}

if [[ -z "$ACC_ID" || -z "$API_TOKEN" || -z "$DB_ID" ]]; then
  echo "Usage: $0 <ACCOUNT_ID> <API_TOKEN> <DATABASE_ID>" >&2
  exit 1
fi

# Legacy variables mapped for current function APIs (email unused)
CF_EMAIL=""
CF_API_KEY="$API_TOKEN"

# Build release
make release

# Paths
DUCKDB_BIN=./build/release/duckdb
EXT=./build/release/extension/cloudflare_d1/cloudflare_d1.duckdb_extension

# Seed via D1 import API (deterministic schema + data), fallback to SQL if it fails
TMP_SQL=$(mktemp -t d1_seed)
cat > "$TMP_SQL" <<'EOSQL'
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO users(id,name,email) VALUES (1,'alice','a@x.com'),(2,'bob','b@x.com');
EOSQL

IMPORT_FAILED=0
if command -v curl >/dev/null 2>&1; then
  curl -sS -X POST \
    -H "Authorization: Bearer $API_TOKEN" \
    -F "file=@$TMP_SQL;type=text/plain" \
    "https://api.cloudflare.com/client/v4/accounts/$ACC_ID/d1/database/$DB_ID/import" >/tmp/d1_import_resp.json || IMPORT_FAILED=1
else
  IMPORT_FAILED=1
fi

if [[ $IMPORT_FAILED -ne 1 ]] && jq -e '.success == true' </tmp/d1_import_resp.json >/dev/null 2>&1; then
  echo "D1 import API seeding succeeded"
else
  echo "D1 import API seeding failed or unavailable, falling back to SQL-based seeding" >&2
  # Create table
  $DUCKDB_BIN -unsigned <<SQL
LOAD '$EXT';
INSTALL json; LOAD json;
SELECT cloudflare_d1('Seed');
SELECT d1_execute('CREATE TABLE IF NOT EXISTS "users" (id INTEGER PRIMARY KEY, name TEXT, email TEXT);', '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID');
SQL

  # Poll sqlite_master for table presence (up to 60 tries)
  for i in {1..60}; do
    ROWS=$($DUCKDB_BIN -unsigned -csv <<SQL
LOAD '$EXT';
SELECT * FROM d1_query('/*object*/ PRAGMA table_info("users")', NULL, '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID') LIMIT 1;
SELECT 1 FROM d1_query('/*object*/ SELECT 1 FROM sqlite_master WHERE type = ''table'' AND name = ''users''', NULL, '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID') LIMIT 1;
SQL
)
    if echo "$ROWS" | tail -n1 | grep -q '^1$'; then
      break
    fi
    sleep 1
    if [[ $i -eq 60 ]]; then
      echo "Timed out waiting for users table to appear" >&2
      exit 1
    fi
  done

  # Insert rows
  $DUCKDB_BIN -unsigned <<SQL
LOAD '$EXT';
SELECT d1_execute('INSERT INTO "users"(id,name,email) VALUES (1,''alice'',''a@x.com''),(2,''bob'',''b@x.com'');', '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID');
SQL
fi

# Verify with extension
$DUCKDB_BIN -unsigned <<SQL
LOAD '$EXT';
INSTALL json; LOAD json;
SELECT * FROM d1_query('SELECT id, name, email FROM users ORDER BY id', NULL, '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID');
SQL

# Run SQL tests
make test

# Pseudo-attach: generate and execute views under alias d1
$DUCKDB_BIN -unsigned <<SQL
LOAD '$EXT';
CREATE SCHEMA IF NOT EXISTS d1;
.mode list
.headers off
.output /tmp/d1_attach.sql
SELECT sql FROM d1_attach_sql('d1', '$ACC_ID', '$CF_EMAIL', '$CF_API_KEY', '$DB_ID');
.output stdout
.read /tmp/d1_attach.sql
-- list schemas and a sample select
SELECT * FROM d1.users ORDER BY id;
SQL

