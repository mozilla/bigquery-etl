#!/usr/bin/env bash
#
# Run lineage/probes + classification (with each model) for one or more
# BigQuery tables.
#
# PREREQUISITE: each table must already be profiled into the column-profiles
# table (mozdata-nonprod.analysis.akomar_column_profiles_v1). Profiling is a
# separate, dataset-scoped step. Use script/metadata/classify_dataset.sh to
# profile a dataset and classify its tables in one go, or run the profiler
# (column_profiles_v1/query.py) directly first. Tables with no profiling rows are
# warned about and skipped (when the bq CLI is available to check).
#
# Run `script/metadata/classification/compare_models.py --table <table>`
# separately to diff two model runs.
#
# Usage:
#   script/metadata/classify_table.sh <project.dataset.table> [<project.dataset.table> ...]
#
# Required env vars:
#   ANTHROPIC_API_KEY   for Claude models
#   DATAHUB_GMS_TOKEN   for lineage_probe_fetcher
# Required for Gemini models:
#   `gcloud auth application-default login` (Vertex AI)
#
# Override the model list via $MODELS (space-separated). Default is a single
# Gemini run. To also classify with Claude (and enable a model comparison
# afterwards), set:
#   MODELS="claude-sonnet-4-6 gemini-3.1-flash-lite-preview"

set -euo pipefail

MODELS="${MODELS:-gemini-3.1-flash-lite-preview}"

# Set REFRESH=1 to re-fetch probes and re-classify, replacing cached rows (use
# after changing fetch/classify logic; otherwise cached rows are reused). Passed
# through to both python steps as --refresh; left empty it expands to nothing.
REFRESH_ARG=""
[[ -n "${REFRESH:-}" ]] && REFRESH_ARG="--refresh"

# Set SANITIZE_REPORT=<path> to append a JSONL record per masked/dropped sample
# value (raw, clean, infoTypes) for inspection. Passed through to the classifier;
# left empty it expands to nothing. The path must not contain spaces.
SANITIZE_ARG=""
[[ -n "${SANITIZE_REPORT:-}" ]] && SANITIZE_ARG="--sanitize-report ${SANITIZE_REPORT}"

# DLP runs in DLP_PROJECT (billed to DLP_QUOTA_PROJECT). Needed when the output
# project (CLASSIFICATION_PROJECT) has no DLP access, e.g. writing to a restricted
# prod dataset while DLP runs in a sandbox. Default (unset) lets the classifier
# fall back to CLASSIFICATION_PROJECT.
DLP_ARGS=""
[[ -n "${DLP_PROJECT:-}" ]] && DLP_ARGS="--dlp-project ${DLP_PROJECT}"
[[ -n "${DLP_QUOTA_PROJECT:-}" ]] && DLP_ARGS="${DLP_ARGS} --dlp-quota-project ${DLP_QUOTA_PROJECT}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <project.dataset.table> [<project.dataset.table> ...]" >&2
    echo "Set \$MODELS to override the model list (default: $MODELS)." >&2
    exit 1
fi

# Resolve repo root so this works no matter where it's invoked from.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Prefer the repo venv so callers need not activate it first.
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
    PYTHON=python
    [[ -x venv/bin/python ]] && PYTHON=venv/bin/python
fi

# Profiling table the classifier reads from (project/dataset/table all
# configurable). PROFILES_TABLE must match what classify_dataset.sh profiled into
# and what field_classifier.py reads. Used to guard against classifying tables
# that were never profiled.
CLASSIFICATION_PROJECT="${CLASSIFICATION_PROJECT:-mozdata-nonprod}"
CLASSIFICATION_DATASET="${CLASSIFICATION_DATASET:-analysis}"
PROFILES_TABLE_NAME="${PROFILES_TABLE:-akomar_column_profiles_v1}"
PROFILES_FQN="${CLASSIFICATION_PROJECT}.${CLASSIFICATION_DATASET}.${PROFILES_TABLE_NAME}"

is_profiled() {  # $1 = project.dataset.table; true if it has profiling rows
    command -v bq >/dev/null 2>&1 || return 0  # can't check without bq; don't block
    local proj ds tbl n
    IFS='.' read -r proj ds tbl <<< "$1"
    n="$(bq --project_id="$CLASSIFICATION_PROJECT" --format=csv query --use_legacy_sql=false \
        "SELECT COUNT(*) FROM \`${PROFILES_FQN}\`
         WHERE source_project = '${proj}'
           AND source_dataset = '${ds}'
           AND source_table = '${tbl}'" 2>/dev/null | tail -n1)" || n=""
    [[ "$n" =~ ^[0-9]+$ && "$n" -gt 0 ]]
}

banner() {
    printf '\n========================================================================\n'
    printf '== %s\n' "$1"
    printf '========================================================================\n'
}

for TABLE in "$@"; do
    banner "TABLE: $TABLE"

    if ! is_profiled "$TABLE"; then
        echo "WARNING: $TABLE has no rows in ${PROFILES_FQN}; profile it first" \
             "(e.g. classify_dataset.sh). Skipping." >&2
        continue
    fi

    banner "[1/2] lineage_probe_fetcher.py"
    "$PYTHON" script/metadata/lineage_probe_fetcher.py --table "$TABLE" $REFRESH_ARG

    for MODEL in $MODELS; do
        banner "[2/2] field_classifier.py --model $MODEL"
        "$PYTHON" script/metadata/field_classifier.py --table "$TABLE" --model "$MODEL" $REFRESH_ARG $SANITIZE_ARG $DLP_ARGS
    done

    banner "DONE: $TABLE"
done
