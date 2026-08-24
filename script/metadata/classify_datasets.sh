#!/usr/bin/env bash
#
# Drive classify_dataset.sh over many datasets: resumable, and records a
# per-dataset cost/time metric line so we build an empirical model as we go.
#
# Usage:
#   script/metadata/classify_datasets.sh <dataset> [<dataset> ...]
#   DATASETS_FILE=/path/to/list.json script/metadata/classify_datasets.sh
#   script/metadata/classify_datasets.sh --report        # summarize metrics.jsonl
#
# DATASETS_FILE may be a JSON array of names or a newline-delimited list.
#
# Resumable: a dataset that already has rows in the classifications table is
# skipped (set FORCE=1 to reclassify). A dataset whose run fails is left
# unmarked, so a re-run retries it. Per-dataset stdout+stderr goes to
# $RUNS_DIR/<dataset>.log; one JSON metric line per dataset is appended to
# $METRICS.
#
# Passes through to classify_dataset.sh: MODELS, DLP_PROJECT, DLP_QUOTA_PROJECT,
# CLASSIFICATION_PROJECT, CLASSIFICATION_DATASET, PROFILES_TABLE, SANITIZE_REPORT,
# REFRESH.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLASSIFICATION_PROJECT="${CLASSIFICATION_PROJECT:-mozdata-nonprod}"
CLASSIFICATION_DATASET="${CLASSIFICATION_DATASET:-analysis}"
DEST_FQN="${CLASSIFICATION_PROJECT}.${CLASSIFICATION_DATASET}.akomar_field_classifications_v1"
MODEL_LABEL="${MODELS:-gemini-3.5-flash-lite}"
RUNS_DIR="${RUNS_DIR:-classification_runs}"
METRICS="${METRICS:-$RUNS_DIR/metrics.jsonl}"
mkdir -p "$RUNS_DIR"

PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then PYTHON=python; [[ -x venv/bin/python ]] && PYTHON=venv/bin/python; fi

# --report: aggregate whatever datapoints we have, and extrapolate to a target
# column count (EXTRAPOLATE_COLUMNS, default 114187 = all 158 SA-readable).
if [[ "${1:-}" == "--report" ]]; then
    EXTRAP="${EXTRAPOLATE_COLUMNS:-114187}"
    "$PYTHON" - "$METRICS" "$EXTRAP" <<'PYEOF'
import sys, json, os
path, extrap = sys.argv[1], int(sys.argv[2])
if not os.path.exists(path):
    print(f"no metrics yet at {path}"); sys.exit(0)
rows = [json.loads(l) for l in open(path) if l.strip()]
ok = [r for r in rows if r.get("rc") == 0 and r.get("columns")]
if not ok:
    print(f"{len(rows)} run(s) recorded, none with columns>0 yet"); sys.exit(0)
cols = sum(r["columns"] for r in ok)
gb   = sum(r["gb_scanned"] for r in ok)
sec  = sum(r["wall_seconds"] for r in ok)
print(f"datapoints: {len(ok)} datasets | {cols:,} columns | {gb:,.1f} GB scanned | {sec/60:.1f} min wall")
print(f"rates: {sec/cols:.2f} s/column | {gb/cols:.3f} GB/column | {gb/1024*6.25:.2f} USD scan (on-demand-equiv)")
print(f"\nextrapolation to {extrap:,} columns (@ current rates):")
print(f"  wall: {sec/cols*extrap/3600:.1f} h sequential")
print(f"  scan: {gb/cols*extrap/1024:.1f} TiB  (~${gb/cols*extrap/1024*6.25:,.0f} on-demand-equiv; ~$0 on reserved slots)")
print(f"  LLM:  ~{extrap:,} calls (~10k input tokens each, taxonomy-dominated; confirm model unit price)")
PYEOF
    exit 0
fi

# Export passthrough env for classify_dataset.sh / its python children.
export CLASSIFICATION_PROJECT CLASSIFICATION_DATASET
[[ -n "${MODELS:-}" ]] && export MODELS
[[ -n "${DLP_PROJECT:-}" ]] && export DLP_PROJECT
[[ -n "${DLP_QUOTA_PROJECT:-}" ]] && export DLP_QUOTA_PROJECT
[[ -n "${PROFILES_TABLE:-}" ]] && export PROFILES_TABLE
[[ -n "${SANITIZE_REPORT:-}" ]] && export SANITIZE_REPORT
[[ -n "${REFRESH:-}" ]] && export REFRESH

# Resolve the dataset list from args or DATASETS_FILE.
DATASETS=()
if [[ $# -gt 0 ]]; then
    DATASETS=("$@")
elif [[ -n "${DATASETS_FILE:-}" ]]; then
    while IFS= read -r d; do [[ -n "$d" ]] && DATASETS+=("$d"); done < <(
        "$PYTHON" - "$DATASETS_FILE" <<'PYEOF'
import sys, json
raw = open(sys.argv[1]).read().strip()
try:
    items = json.loads(raw)
    items = [x.split(".")[-1] for x in items]     # accept fully-qualified names
except Exception:
    items = [l.strip() for l in raw.splitlines() if l.strip()]
print("\n".join(items))
PYEOF
    )
else
    echo "Usage: $0 <dataset> [...]   |   DATASETS_FILE=list.json $0   |   $0 --report" >&2
    exit 1
fi

echo "Datasets to consider: ${#DATASETS[@]}  |  dest: ${DEST_FQN}  |  model: ${MODEL_LABEL}"

already_classified() {  # $1 = bare dataset name
    local n
    n="$(bq --project_id="$CLASSIFICATION_PROJECT" --format=csv query --use_legacy_sql=false --max_rows=1 \
        "SELECT COUNT(*) FROM \`${DEST_FQN}\` WHERE source_dataset = '$1'" 2>/dev/null | tail -n1)" || n=""
    [[ "$n" =~ ^[0-9]+$ && "$n" -gt 0 ]]
}

banner() { printf '\n========================================================================\n== %s\n========================================================================\n' "$1"; }

n_done=0 n_skip=0 n_fail=0
for DS in "${DATASETS[@]}"; do
    if [[ -z "${FORCE:-}" ]] && already_classified "$DS"; then
        echo "SKIP  $DS (already classified; FORCE=1 to redo)"
        n_skip=$((n_skip+1)); continue
    fi
    banner "CLASSIFY $DS"
    LOG="$RUNS_DIR/${DS}.log"
    start=$(date +%s)
    rc=0
    script/metadata/classify_dataset.sh "$DS" >"$LOG" 2>&1 || rc=$?
    elapsed=$(( $(date +%s) - start ))

    "$PYTHON" - "$DS" "$LOG" "$elapsed" "$rc" "$MODEL_LABEL" >>"$METRICS" <<'PYEOF'
import sys, re, json
ds, log, secs, rc, model = sys.argv[1:6]
t = open(log, encoding="utf-8", errors="replace").read()
def snum(pat, cast=float):
    return sum(cast(x) for x in re.findall(pat, t)) if re.search(pat, t) else 0
rec = {
    "dataset": ds, "rc": int(rc), "wall_seconds": int(secs),
    "gb_scanned": round(snum(r"([\d.]+) GB scanned"), 2),
    "columns": int(snum(r"Done \S+ \((\d+) columns\)", int)),
    "masked": int(snum(r"sanitized samples: (\d+) masked", int)),
    "dropped": int(snum(r"(\d+) dropped across", int)),
    "suppressed": int(snum(r"suppressed samples for (\d+) high", int)),
    "failed_calls": len(re.findall(r"call failed for", t)),
    "model": model,
}
print(json.dumps(rec))
sys.stderr.write(
    f"  -> {rec['columns']} cols, {rec['gb_scanned']} GB, {secs}s, "
    f"masked {rec['masked']}, suppressed {rec['suppressed']}, failed {rec['failed_calls']}, rc {rc}\n")
PYEOF

    if [[ $rc -eq 0 ]]; then n_done=$((n_done+1)); else n_fail=$((n_fail+1)); echo "  FAILED (rc=$rc) - see $LOG" >&2; fi
done

banner "SUMMARY: $n_done classified, $n_skip skipped, $n_fail failed"
echo "metrics: $METRICS   (run '$0 --report' for aggregate cost/time)"
