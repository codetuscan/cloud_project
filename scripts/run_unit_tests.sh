#!/usr/bin/env bash
# Runs the pytest unit tests inside the project image (no cluster needed).  ./scripts/run_unit_tests.sh
set -e
cd "$(dirname "$0")/.."
docker compose build controller >/dev/null
docker compose run --rm --no-deps -v "$PWD/tests:/srv/tests" -v "$PWD/pytest.ini:/srv/pytest.ini" \
    --entrypoint python controller -m pytest -q "$@"
