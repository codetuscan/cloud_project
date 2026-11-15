#!/usr/bin/env bash
# Fresh deployment: removes the Compose project's containers AND volumes (all stored data!), rebuilds, starts.
#   ./scripts/clean_start.sh
set -e
cd "$(dirname "$0")/.."
echo "This deletes all objects stored by this project (Docker volumes of the objstore Compose project)."
if [ "${1:-}" != "--yes" ]; then read -r -p "Continue? [y/N] " a; [ "$a" = y ] || exit 1; fi
docker compose down -v
docker compose up -d --build
echo -n "Waiting for the cluster"
for i in $(seq 1 60); do
  n=$(curl -s localhost:${CONTROLLER_PORT:-8090}/internal/status 2>/dev/null | python3 -c "import sys,json;print(sum(1 for w in json.load(sys.stdin)['workers'] if w['state']=='HEALTHY'))" 2>/dev/null || echo 0)
  [ "$n" = 4 ] && { echo " — ready (4 workers HEALTHY)"; exit 0; }
  echo -n "."; sleep 2
done
echo " — timed out"; exit 1
