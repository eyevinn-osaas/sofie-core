#!/bin/bash
# OSC entrypoint for Sofie Core.
# - maps OSC platform conventions (PORT, OSC_HOSTNAME, DATABASE_URL) to what Sofie Core reads
# - enables the polling replacement for MongoDB change streams (FerretDB has no change streams)
# - prepares the persistent volume and drops root privileges
set -e

# Port: Sofie reads SOFIE_PORT, not PORT
export SOFIE_PORT="${PORT:-8080}"
export SOFIE_BIND_ADDRESS="${SOFIE_BIND_ADDRESS:-0.0.0.0}"

# Public URL (must not contain a path)
if [ -n "${OSC_HOSTNAME:-}" ]; then
	export ROOT_URL="${ROOT_URL:-https://${OSC_HOSTNAME}}"
else
	export ROOT_URL="${ROOT_URL:-http://localhost:${SOFIE_PORT}}"
fi
# Running behind the OSC ingress
export HTTP_FORWARDED_COUNT="${HTTP_FORWARDED_COUNT:-1}"

# Database: MONGO_URL wins, otherwise the OSC FerretDB connection string in DATABASE_URL.
# Sofie requires a database name in the URL, so add one (default "sofie") if missing.
DB_URL="${MONGO_URL:-${DATABASE_URL:-}}"
if [ -z "$DB_URL" ]; then
	echo "osc-entrypoint: MONGO_URL or DATABASE_URL must be set (connection string of an OSC FerretDB instance)" >&2
	exit 1
fi
DB_NAME="${DATABASE_NAME:-sofie}"
base="${DB_URL%%\?*}"
query=""
if [ "$base" != "$DB_URL" ]; then query="?${DB_URL#*\?}"; fi
rest="${base#*://}"
if [[ "$rest" == */* && -n "${rest#*/}" ]]; then
	: # already has a database name
else
	base="${base%/}/${DB_NAME}"
fi
export MONGO_URL="${base}${query}"

# FerretDB does not support change streams: poll the database instead (see packages/corelib/src/pollingChangeStream.ts)
export SOFIE_DB_CHANGE_POLLING="${SOFIE_DB_CHANGE_POLLING:-true}"
export SOFIE_DB_POLL_INTERVAL_MS="${SOFIE_DB_POLL_INTERVAL_MS:-2000}"

# Persistent storage (snapshots etc.)
export SOFIE_STORE_PATH="${SOFIE_STORE_PATH:-/data/sofie-store}"
mkdir -p "$SOFIE_STORE_PATH"

cd /opt/core/meteor
if [ "$(id -u)" = "0" ]; then
	chown -R 1000:1000 "$SOFIE_STORE_PATH" 2>/dev/null || true
	exec su-exec 1000:1000 "$@"
fi
exec "$@"
