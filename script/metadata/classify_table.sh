#!/usr/bin/env bash
#
# Run the classification sequence (profile → lineage/probes → classify with
# each model) for one or more BigQuery tables. Run
# `script/metadata/classification/compare_models.py --table <table>`
# separately to diff two model runs.
#
# Usage:
#   script/metadata/classify_table.sh <project.dataset.table> [<project.dataset.table> ...]
#
# Required env vars:
#   ANTHROPIC_API_KEY   — for Claude models
#   DATAHUB_GMS_TOKEN   — for lineage_probe_fetcher
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

banner() {
    printf '\n========================================================================\n'
    printf '== %s\n' "$1"
    printf '========================================================================\n'
}

for TABLE in "$@"; do
    banner "TABLE: $TABLE"

    banner "[1/3] field_profiler.py"
    python script/metadata/field_profiler.py --table "$TABLE"

    banner "[2/3] lineage_probe_fetcher.py"
    python script/metadata/lineage_probe_fetcher.py --table "$TABLE"

    for MODEL in $MODELS; do
        banner "[3/3] field_classifier.py --model $MODEL"
        python script/metadata/field_classifier.py --table "$TABLE" --model "$MODEL"
    done

    banner "DONE: $TABLE"
done
