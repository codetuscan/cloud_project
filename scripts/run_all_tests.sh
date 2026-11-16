#!/usr/bin/env bash
# Runs the complete verification suite: unit tests, every phase's end-to-end script, persistence test.
# Takes ~35-45 minutes. Stack must be running (docker compose up -d --build).  ./scripts/run_all_tests.sh
cd "$(dirname "$0")/.."
declare -a names results
run() { echo; echo "################ $1"; shift; if "$@"; then results+=("PASS"); else results+=("FAIL"); fi; names+=("$LAST"); }
for t in "unit tests:./scripts/run_unit_tests.sh" \
         "phase 2+3 worker storage, heartbeats:./scripts/phase2_3_test.sh" \
         "phase 4 upload path:./scripts/phase4_test.sh" \
         "phase 5 download/list/patch/delete:./scripts/phase5_test.sh" \
         "phase 6 failures + repair:./scripts/phase6_test.sh" \
         "phase 7 scrubbers:./scripts/phase7_test.sh" \
         "phase 8 caching:./scripts/phase8_test.sh" \
         "phase 9 concurrency + memory:./scripts/phase9_test.sh" \
         "phase 10 restarts + persistence:./scripts/phase10_persistence_test.sh" \
         "phase 11 upload crash safety:./scripts/phase11_upload_crash_test.sh"; do
  LAST=${t%%:*}; run "$LAST" ${t#*:}
done
echo; echo "================ SUMMARY"
rc=0; for i in "${!names[@]}"; do printf '  %-6s %s\n' "${results[$i]}" "${names[$i]}"; [ "${results[$i]}" = PASS ] || rc=1; done
exit $rc
