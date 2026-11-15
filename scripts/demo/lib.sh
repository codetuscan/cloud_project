# Shared helpers for the demo scripts (sourced, not executed).
API=${API:-http://localhost:8090}
WORKERS="worker1 worker2 worker3 worker4"
BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'; YEL=$'\033[33m'; RST=$'\033[0m'
DEMO_TMP=${DEMO_TMP:-$(mktemp -d)}

banner() { echo; echo "${BOLD}━━━ $* ━━━${RST}"; }
step()   { echo; echo "${BOLD}▶ $*${RST}"; }
note()   { echo "${DIM}  $*${RST}"; }
good()   { echo "  ${GREEN}✔ $*${RST}"; }
badmsg() { echo "  ${RED}✘ $*${RST}"; DEMO_FAILED=1; }
show()   { echo "${DIM}  \$ $*${RST}"; }
jget()   { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
sql()    { docker exec postgres psql -U objstore -tAc "$1"; }
expect_eq() { if [ "$2" = "$3" ]; then good "$1"; else badmsg "$1 (expected '$3', got '$2')"; fi; }
wait_for() {  # wait_for "<what>" <seconds> <condition...>
  local what="$1" t="$2"; shift 2; local i
  for ((i=0;i<t;i+=2)); do eval "$@" >/dev/null 2>&1 && { good "$what (after ~${i}s)"; return 0; }; sleep 2; done
  badmsg "$what (gave up after ${t}s)"; return 1; }
wait_cluster() { wait_for "all 4 workers HEALTHY" 90 "[ \"\$(curl -s $API/internal/status | jget \"sum(1 for w in d['workers'] if w['state']=='HEALTHY')\")\" = 4 ]"; }
upload() {    # upload <file> [metadata-json]  -> prints the full JSON response
  if [ -n "${2:-}" ]; then curl -s -F "file=@$1" -F "metadata=$2" $API/api/v1/objects; else curl -s -F "file=@$1" $API/api/v1/objects; fi; }
placement_table() {  # placement_table <object_id>: one line per chunk with its replica workers
  curl -s $API/api/v1/objects/$1/metadata | jget "'\n'.join('    chunk %d  %8d bytes  sha256:%s…  ->  %s'%(c['index'],c['size'],c['checksum'][7:19],', '.join('%s(%s)'%(r['worker_id'],r['state']) for r in c['replicas'])) for c in d['chunks'])"; }
cluster_summary() {
  curl -s $API/internal/status | jget "'    workers: '+', '.join('%s=%s'%(w['worker_id'],w['state']) for w in d['workers'])+'\n    replicas: %s | under-replicated chunks: %d | repair tasks: %s'%(d['replicas'],d['under_replicated_chunks'],d['repair_tasks'])"; }
finish() { echo; if [ "${DEMO_FAILED:-0}" = 0 ]; then echo "${GREEN}${BOLD}DEMO OK${RST}"; else echo "${RED}${BOLD}DEMO HAD FAILURES${RST}"; exit 1; fi; }
