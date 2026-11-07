#!/usr/bin/env bash
# Nightly Postgres backup for the production stack. Run from the repository directory:
#
#   ./deploy/backup.sh                 # writes backups/shop-YYYYmmdd-HHMMSS.sql.gz
#   KEEP_DAYS=14 ./deploy/backup.sh
#
# Postgres is the system of record. Elasticsearch and MongoDB are derived and can be rebuilt from
# it, so they are deliberately not backed up here (see docs/deployment.md, "Backups and restore").
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.prod.yml --env-file .env.prod)
BACKUP_DIR="${BACKUP_DIR:-backups}"
KEEP_DAYS="${KEEP_DAYS:-7}"
umask 077
mkdir -p "$BACKUP_DIR"

out="$BACKUP_DIR/shop-$(date +%Y%m%d-%H%M%S).sql.gz"
tmp="$out.partial"
trap 'rm -f "$tmp"' EXIT

# pg_dump runs inside the container, using the credentials already in its environment.
"${COMPOSE[@]}" exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner' \
  | gzip -9 > "$tmp"

# An empty or truncated dump must never replace a good one or count as a backup.
gzip -t "$tmp"
if [ "$(gzip -dc "$tmp" | wc -c)" -lt 1000 ]; then
  echo "backup looks empty, refusing to keep it" >&2
  exit 1
fi
mv "$tmp" "$out"
trap - EXIT

find "$BACKUP_DIR" -name 'shop-*.sql.gz' -mtime "+$KEEP_DAYS" -delete
echo "wrote $out ($(du -h "$out" | cut -f1))"
