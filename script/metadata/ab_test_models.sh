#!/usr/bin/env bash
#
# Model A/B: classify one dataset (default: the vetted FxA accounts_db_external)
# with each candidate model, reusing existing profiles so there is NO re-profiling
# / re-scan. Records per-model runtime, token usage, and estimated cost, and keeps
# a full per-model log for later review.
#
# ONE COMMAND runs all models. Results:
#   classification_runs/model_ab/<model>.log   full stdout+stderr per model
#   classification_runs/model_ab/ab_metrics.jsonl   one JSON line per model run
#   classification_runs/model_ab/ab_summary.txt      the comparison table
#   classification_runs/model_ab/ab_run.log          everything, tee'd
#
# Each model's rows coexist in the classifications table (distinguished by the
# `model` column), so nothing is overwritten. Re-running skips models/columns
# already classified (per-model cache); set REFRESH=1 to re-measure from scratch.
#
# Usage:
#   script/metadata/ab_test_models.sh
#   MODELS="gemini-3.6-flash claude-sonnet-5" script/metadata/ab_test_models.sh   # subset
#
# Required env:
#   ANTHROPIC_API_KEY   for the claude-* arms
#   Vertex ADC (gcloud auth application-default login) for the gemini-* arms
# Recommended env (same as the FxA run):
#   DLP_PROJECT / DLP_QUOTA_PROJECT   run DLP sanitization in a project that has it
#   CLASSIFICATION_PROJECT / CLASSIFICATION_DATASET   where profiles/output live

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# --- config ---
MODELS="${MODELS:-gemini-3.5-flash-lite gemini-3.6-flash claude-haiku-4-5 claude-sonnet-5 claude-opus-5}"
SOURCE_PROJECT="${SOURCE_PROJECT:-moz-fx-data-shared-prod}"
DATASET="${DATASET:-accounts_db_external}"
CLASSIFICATION_PROJECT="${CLASSIFICATION_PROJECT:-moz-fx-data-shared-prod}"
CLASSIFICATION_DATASET="${CLASSIFICATION_DATASET:-data_governance_metadata_derived}"
PROFILES_TABLE="${PROFILES_TABLE:-akomar_column_profiles_v1}"
export CLASSIFICATION_PROJECT CLASSIFICATION_DATASET PROFILES_TABLE
# Forward DLP + refresh + sanitize-report to the classify_table.sh children.
[[ -n "${DLP_PROJECT:-}" ]] && export DLP_PROJECT
[[ -n "${DLP_QUOTA_PROJECT:-}" ]] && export DLP_QUOTA_PROJECT
[[ -n "${SANITIZE_REPORT:-}" ]] && export SANITIZE_REPORT
[[ -n "${REFRESH:-}" ]] && export REFRESH

PROFILES_FQN="${CLASSIFICATION_PROJECT}.${CLASSIFICATION_DATASET}.${PROFILES_TABLE}"
RUNS_DIR="${RUNS_DIR:-classification_runs/model_ab}"
mkdir -p "$RUNS_DIR"
METRICS="$RUNS_DIR/ab_metrics.jsonl"
SUMMARY="$RUNS_DIR/ab_summary.txt"
MAIN_LOG="$RUNS_DIR/ab_run.log"

PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then PYTHON=python; [[ -x venv/bin/python ]] && PYTHON=venv/bin/python; fi

banner() { printf '\n========================================================================\n== %s\n========================================================================\n' "$1"; }

# Tee everything below to the main log for later review.
exec > >(tee -a "$MAIN_LOG") 2>&1

banner "MODEL A/B  dataset=${DATASET}  models: ${MODELS}"
echo "profiles: ${PROFILES_FQN}   output: ${CLASSIFICATION_PROJECT}.${CLASSIFICATION_DATASET}.akomar_field_classifications_v1"

# Warn (don't fail) if a claude arm is requested without an API key.
if [[ "$MODELS" == *claude-* && -z "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "WARNING: MODELS includes a claude-* model but ANTHROPIC_API_KEY is unset - those arms will fail." >&2
fi

# Enumerate the tables already profiled for this dataset (no re-profiling).
banner "enumerating profiled tables in ${DATASET}"
TABLES=()
while IFS= read -r _t; do [[ -n "$_t" ]] && TABLES+=("${SOURCE_PROJECT}.${DATASET}.${_t}"); done < <(
    bq --project_id="$CLASSIFICATION_PROJECT" --format=csv query --use_legacy_sql=false --max_rows=100000 \
        "SELECT DISTINCT source_table FROM \`${PROFILES_FQN}\`
         WHERE source_dataset = '${DATASET}' ORDER BY source_table" | tail -n +2
)
if [[ ${#TABLES[@]} -eq 0 ]]; then
    echo "No profiled tables for ${DATASET} in ${PROFILES_FQN}. Profile it first (classify_dataset.sh)." >&2
    exit 1
fi
echo "will classify ${#TABLES[@]} table(s) per model"

# --- REFRESH: clear once, up front (parallel-safe) ---
# Under parallelism, classify_table.sh's per-table --refresh would issue concurrent
# DELETEs against one table and hit BigQuery's DML-concurrency limits. So when
# REFRESH is set we clear all A/B rows for this dataset in a SINGLE delete here,
# then drop REFRESH so the parallel children only append (concurrent load jobs are
# safe). Lineage/probe tables stay cached (no re-fetch needed for a re-run).
DEST_FQN="${CLASSIFICATION_PROJECT}.${CLASSIFICATION_DATASET}.akomar_field_classifications_v1"
if [[ -n "${REFRESH:-}" ]]; then
    models_in=""
    for m in $MODELS; do models_in="${models_in}'${m}',"; done
    models_in="${models_in%,}"
    banner "REFRESH: clearing existing rows for ${DATASET} (models: ${MODELS})"
    bq --project_id="$CLASSIFICATION_PROJECT" query --use_legacy_sql=false \
        "DELETE FROM \`${DEST_FQN}\`
         WHERE source_dataset = '${DATASET}' AND model IN (${models_in})"
    unset REFRESH
fi

# Classify one model: full per-model log + a metrics fragment. Parallel-safe -
# each model writes its own log and its own <model>.metrics.json (no shared-file
# append races); fragments are assembled after all finish.
run_one_model() {
    local MODEL="$1"
    local LOG="$RUNS_DIR/${MODEL}.log"
    local FRAG="$RUNS_DIR/${MODEL}.metrics.json"
    local start rc=0
    start=$(date +%s)
    MODELS="$MODEL" script/metadata/classify_table.sh "${TABLES[@]}" >"$LOG" 2>&1 || rc=$?
    local elapsed=$(( $(date +%s) - start ))
    "$PYTHON" - "$MODEL" "$LOG" "$elapsed" "$rc" >"$FRAG" <<'PYEOF'
import sys, re, json
model, log, secs, rc = sys.argv[1:5]
t = open(log, encoding="utf-8", errors="replace").read()
rec = {
    "model": model, "rc": int(rc), "wall_seconds": int(secs),
    "columns": sum(int(x) for x in re.findall(r"Done \S+ \((\d+) columns\)", t)),
    "llm_calls": sum(int(x) for x in re.findall(r"llm_calls=(\d+)", t)),
    "input_tokens": sum(int(x) for x in re.findall(r"input_tokens=(\d+)", t)),
    "output_tokens": sum(int(x) for x in re.findall(r"output_tokens=(\d+)", t)),
    "failed_calls": len(re.findall(r"call failed for", t)),
}
print(json.dumps(rec))
PYEOF
    echo "  finished ${MODEL}: rc=${rc}, ${elapsed}s (log: ${LOG})"
}

# --- run all models (parallel by default; PARALLEL=0 for sequential) ---
if [[ "${PARALLEL:-1}" == "1" ]]; then
    banner "running ${#TABLES[@]} tables x $(echo $MODELS | wc -w | tr -d ' ') models IN PARALLEL"
    for MODEL in $MODELS; do
        run_one_model "$MODEL" &
        echo "  launched ${MODEL} (pid $!) -> $RUNS_DIR/${MODEL}.log"
    done
    wait
else
    banner "running models SEQUENTIALLY"
    for MODEL in $MODELS; do banner "MODEL: ${MODEL}"; run_one_model "$MODEL"; done
fi

# Assemble per-model fragments into the metrics file the summary reads.
cat "$RUNS_DIR"/*.metrics.json > "$METRICS" 2>/dev/null || true

# --- summary table + cost ---
banner "SUMMARY"
"$PYTHON" - "$METRICS" "$SUMMARY" <<'PYEOF'
import sys, json, os
metrics_path, summary_path = sys.argv[1], sys.argv[2]

# Price per 1M tokens (input, output). Standard, global-endpoint, <=200K-token
# tier (our prompts are ~10K tokens, well under the 200K break). Batch/Flex is
# ~50% off and context-caching cuts cached input further - both apply to prod, not
# this A/B; see MODEL_AB_TESTING.md.
# Gemini: Vertex pricing, confirmed 2026-07-31
#   https://cloud.google.com/gemini-enterprise-agent-platform/generative-ai/pricing
#   (non-global endpoints are ~10% higher starting 2026-07-01).
# Claude: Anthropic list. Sonnet 5 has an intro rate of 2/10 through 2026-08-31;
#   standard 3/15 shown here (conservative).
PRICES = {
    "gemini-3.5-flash-lite":         (0.30, 2.50, False),   # (in, out, unverified)
    "gemini-3.1-flash-lite":         (0.25, 1.50, False),   # GA (the -preview id is retired/404)
    "gemini-3.6-flash":              (1.50, 7.50, False),
    "claude-haiku-4-5":              (1.00, 5.00, False),
    "claude-sonnet-5":               (3.00, 15.00, False),
    "claude-opus-5":                 (5.00, 25.00, False),
}
EXTRAP = int(os.environ.get("EXTRAPOLATE_COLUMNS", "114187"))  # all SA-readable cols

# last line wins per model (handles re-runs)
rows = {}
for line in open(metrics_path):
    line = line.strip()
    if line:
        r = json.loads(line)
        rows[r["model"]] = r

def cost(model, r):
    p = PRICES.get(model)
    if not p:
        return None, False
    cin, cout, unver = p
    c = r["input_tokens"] / 1e6 * cin + r["output_tokens"] / 1e6 * cout
    return c, unver

hdr = f"{'model':<32} {'cols':>5} {'wall':>7} {'s/col':>6} {'in_tok':>10} {'out_tok':>8} {'cost$':>8} {'$/1k':>7} {'~full$':>9}  notes"
lines = [hdr, "-" * len(hdr)]
for model in rows:
    r = rows[model]
    c, unver = cost(model, r)
    cols = r["columns"] or 0
    spc = (r["wall_seconds"] / cols) if cols else 0
    per1k = (c / cols * 1000) if (c is not None and cols) else None
    full = (c / cols * EXTRAP) if (c is not None and cols) else None
    notes = []
    if unver: notes.append("PRICE UNVERIFIED")
    if r["rc"] != 0: notes.append(f"rc={r['rc']}")
    failed = r.get("failed_calls", r.get("transient_errors", 0))
    if failed: notes.append(f"{failed} failed-calls")
    lines.append(
        f"{model:<32} {cols:>5} {r['wall_seconds']:>6}s {spc:>6.1f} "
        f"{r['input_tokens']:>10} {r['output_tokens']:>8} "
        f"{('%.2f' % c) if c is not None else '   n/a':>8} "
        f"{('%.3f' % per1k) if per1k is not None else '  n/a':>7} "
        f"{('%.0f' % full) if full is not None else '   n/a':>9}  {', '.join(notes)}")

out = "\n".join(lines)
out += ("\n\ncost$   = actual spend to classify this dataset (from real token counts)"
        f"\n~full$ = extrapolation to {EXTRAP:,} columns at the same per-column rate")
print(out)
open(summary_path, "w").write(out + "\n")
print(f"\nwritten: {summary_path}")
PYEOF

banner "DONE  (full log: ${MAIN_LOG})"
