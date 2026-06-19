#!/usr/bin/env bash
#
# Run lineage/probes + classification (with each model) for one or more
# BigQuery tables.
#
# PREREQUISITE: each table must already be profiled into the column-profiles
# table (mozdata-nonprod.analysis.akomar_column_profiles_v1). Profiling is a
# separate, dataset-scoped step. Use script/metadata/classify_dataset.sh to
# profile a dataset and classify its tables in one go, or run the profiler
# (column_profiles_v1/query.py) directly first.
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

banner() {
    printf '\n========================================================================\n'
    printf '== %s\n' "$1"
    printf '========================================================================\n'
}

for TABLE in "$@"; do
    banner "TABLE: $TABLE"

    banner "[1/2] lineage_probe_fetcher.py"
    "$PYTHON" script/metadata/lineage_probe_fetcher.py --table "$TABLE"

    for MODEL in $MODELS; do
        banner "[2/2] field_classifier.py --model $MODEL"
        "$PYTHON" script/metadata/field_classifier.py --table "$TABLE" --model "$MODEL"
    done

    banner "DONE: $TABLE"
done
