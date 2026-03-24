#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

POSTGRES_URL="${POSTGRES_SMOKE_URL:-}"
MYSQL_URL="${MYSQL_SMOKE_URL:-}"
ALLOW_DESTRUCTIVE=0
RUN_POSTGRES=0
RUN_MYSQL=0
WAIT_FOR_DB=0
WAIT_TIMEOUT_SECONDS="${SMOKE_WAIT_TIMEOUT_SECONDS:-60}"
WAIT_INTERVAL_SECONDS="${SMOKE_WAIT_INTERVAL_SECONDS:-2}"

usage() {
  cat <<'EOF'
Usage: scripts/smoke-test.sh [options]

Options:
  --postgres-url <url>     Postgres database URL to test
  --mysql-url <url>        MySQL database URL to test
  --postgres-only          Run only the Postgres smoke test
  --mysql-only             Run only the MySQL smoke test
  --allow-destructive      Also run fresh and wipe (use only on disposable databases)
  --wait                   Wait for each database to become ready before testing
  --wait-timeout <secs>    Max seconds to wait for readiness (default: 60)
  --wait-interval <secs>   Poll interval while waiting (default: 2)
  -h, --help               Show this help

Environment:
  POSTGRES_SMOKE_URL       Default Postgres URL
  MYSQL_SMOKE_URL          Default MySQL URL
  SMOKE_WAIT_TIMEOUT_SECONDS   Default readiness timeout
  SMOKE_WAIT_INTERVAL_SECONDS  Default readiness poll interval

Notes:
  - Non-destructive mode tests: migrate, status, show, table, rollback, reset, and JSON output.
  - Destructive mode additionally tests: fresh and wipe.
  - fresh/wipe will remove all tables in the target database. Use a disposable database only.
  - --wait forwards readiness polling flags to dbrs itself.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --postgres-url)
      POSTGRES_URL="${2:?missing value for --postgres-url}"
      shift 2
      ;;
    --mysql-url)
      MYSQL_URL="${2:?missing value for --mysql-url}"
      shift 2
      ;;
    --postgres-only)
      RUN_POSTGRES=1
      shift
      ;;
    --mysql-only)
      RUN_MYSQL=1
      shift
      ;;
    --allow-destructive)
      ALLOW_DESTRUCTIVE=1
      shift
      ;;
    --wait)
      WAIT_FOR_DB=1
      shift
      ;;
    --wait-timeout)
      WAIT_TIMEOUT_SECONDS="${2:?missing value for --wait-timeout}"
      shift 2
      ;;
    --wait-interval)
      WAIT_INTERVAL_SECONDS="${2:?missing value for --wait-interval}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ $RUN_POSTGRES -eq 0 && $RUN_MYSQL -eq 0 ]]; then
  RUN_POSTGRES=1
  RUN_MYSQL=1
fi

if [[ $RUN_POSTGRES -eq 1 && -z "$POSTGRES_URL" ]]; then
  echo "Skipping Postgres: no URL set (use --postgres-url or POSTGRES_SMOKE_URL)."
  RUN_POSTGRES=0
fi

if [[ $RUN_MYSQL -eq 1 && -z "$MYSQL_URL" ]]; then
  echo "Skipping MySQL: no URL set (use --mysql-url or MYSQL_SMOKE_URL)."
  RUN_MYSQL=0
fi

if [[ $RUN_POSTGRES -eq 0 && $RUN_MYSQL -eq 0 ]]; then
  echo "Nothing to run." >&2
  exit 1
fi

run_cmd() {
  echo "+ $*"
  "$@"
}

run_backend() {
  local backend="$1"
  local url="$2"
  local slug="$3"
  local backend_label
  backend_label="$(printf '%s' "$backend" | tr '[:lower:]' '[:upper:]')"

  local dir table migration_table
  dir="$(mktemp -d "/tmp/dbrs-${slug}-XXXXXX")"
  trap 'rm -rf "$dir"' RETURN

  table="${slug}_widgets"
  migration_table="dbrs_${slug}_$(date +%s)"

  echo
  echo "=== ${backend_label} smoke test ==="
  echo "database_url=$url"
  echo "migration_table=$migration_table"
  echo "table=$table"
  echo "dir=$dir"

  run_cmd cargo run -q -- new "create-${table}-table" --table --backend "$backend" --dir "$dir"

  sleep 1
  local second
  second="$(printf '%s/%s-add-%s-name.sql' "$dir" "$(date +%Y_%m_%d_%H%M%S)" "$table")"

  if [[ "$backend" == "postgres" ]]; then
    cat > "$second" <<EOF
-- dbrs:up

ALTER TABLE "$table"
ADD COLUMN name TEXT NOT NULL DEFAULT 'unnamed';

CREATE UNIQUE INDEX ${table}_name_idx ON "$table" (name);

INSERT INTO "$table" (name) VALUES ('alpha');

-- dbrs:down

DROP INDEX ${table}_name_idx;
ALTER TABLE "$table" DROP COLUMN name;
EOF
  else
    cat > "$second" <<EOF
-- dbrs:up

ALTER TABLE \`$table\`
ADD COLUMN name VARCHAR(255) NOT NULL DEFAULT 'unnamed';

CREATE UNIQUE INDEX ${table}_name_idx ON \`$table\` (name);

INSERT INTO \`$table\` (name) VALUES ('alpha');

-- dbrs:down

DROP INDEX ${table}_name_idx ON \`$table\`;
ALTER TABLE \`$table\` DROP COLUMN name;
EOF
  fi

  export DATABASE_URL="$url"
  export DBRS_MIGRATION_TABLE="$migration_table"

  local wait_args=()
  if [[ "$WAIT_FOR_DB" -eq 1 ]]; then
    wait_args+=(--wait --wait-timeout "$WAIT_TIMEOUT_SECONDS" --wait-interval "$WAIT_INTERVAL_SECONDS")
  fi

  run_cmd cargo run -q -- "${wait_args[@]}" migrate --dir "$dir"
  run_cmd cargo run -q -- "${wait_args[@]}" status --dir "$dir"
  run_cmd cargo run -q -- "${wait_args[@]}" show --limit 10
  run_cmd cargo run -q -- "${wait_args[@]}" table "$table"

  run_cmd cargo run -q -- --json "${wait_args[@]}" status --dir "$dir"
  run_cmd cargo run -q -- --json "${wait_args[@]}" show --limit 5
  run_cmd cargo run -q -- --json "${wait_args[@]}" table "$table"

  run_cmd cargo run -q -- "${wait_args[@]}" rollback --dir "$dir" --steps 1 --yes
  run_cmd cargo run -q -- "${wait_args[@]}" status --dir "$dir"
  run_cmd cargo run -q -- "${wait_args[@]}" reset --dir "$dir" --yes
  run_cmd cargo run -q -- "${wait_args[@]}" status --dir "$dir"

  if [[ "$ALLOW_DESTRUCTIVE" -eq 1 ]]; then
    echo "--- destructive checks enabled ---"
    run_cmd cargo run -q -- "${wait_args[@]}" fresh --dir "$dir" --yes
    run_cmd cargo run -q -- "${wait_args[@]}" wipe --yes
  else
    echo "--- destructive checks skipped (pass --allow-destructive to enable fresh/wipe) ---"
  fi

  unset DATABASE_URL
  unset DBRS_MIGRATION_TABLE

  echo "=== ${backend_label} smoke test passed ==="
}

echo "Building dbrs..."
run_cmd cargo build -q

if [[ $RUN_POSTGRES -eq 1 ]]; then
  run_backend postgres "$POSTGRES_URL" pgsmoke
fi

if [[ $RUN_MYSQL -eq 1 ]]; then
  run_backend mysql "$MYSQL_URL" mysqlsmoke
fi

echo
echo "Smoke tests completed successfully."
