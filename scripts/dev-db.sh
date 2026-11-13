#!/usr/bin/env bash
# Start a throwaway local PostgreSQL for development on port 5433.
# Data lives in ./.pgdata; delete that folder to reset everything.
#
#   scripts/dev-db.sh        start (creates the cluster on first run)
#   scripts/dev-db.sh stop   stop
set -euo pipefail
cd "$(dirname "$0")/.."

for dir in /opt/homebrew/opt/postgresql@17/bin /usr/local/opt/postgresql@17/bin; do
  [ -d "$dir" ] && PATH="$dir:$PATH"
done
command -v pg_ctl >/dev/null || { echo "PostgreSQL not found. Try: brew install postgresql@17"; exit 1; }

export LC_ALL=en_US.UTF-8
DATA=.pgdata
PORT=5433

if [ "${1:-start}" = "stop" ]; then
  pg_ctl -D "$DATA" stop -m fast
  exit 0
fi

if [ ! -d "$DATA" ]; then
  initdb -D "$DATA" --locale=C --encoding=UTF8 --auth=trust --username="$USER" >/dev/null
fi
if ! pg_ctl -D "$DATA" status >/dev/null 2>&1; then
  pg_ctl -D "$DATA" -l "$DATA/server.log" -o "-p $PORT -c listen_addresses=localhost -c max_connections=300" -w start >/dev/null
fi
for db in stepwise stepwise_test; do
  psql -p "$PORT" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 \
    || createdb -p "$PORT" "$db"
done
echo "PostgreSQL is running: postgres://localhost:$PORT/stepwise?sslmode=disable"
