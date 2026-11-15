#!/usr/bin/env bash
# Runs every required demonstration from the course spec, in order. Needs the stack running.
cd "$(dirname "$0")"
export DEMO_TMP=$(mktemp -d); trap 'rm -rf $DEMO_TMP' EXIT
fail=0
for d in 01_store_and_retrieve 02_chunking_and_distribution 03_replication 04_metadata 05_worker_failure 06_data_integrity; do
  bash ./$d.sh || fail=1
done
echo; [ $fail = 0 ] && echo "ALL DEMOS PASSED" || { echo "SOME DEMOS FAILED"; exit 1; }
