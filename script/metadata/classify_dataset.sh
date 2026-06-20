#!/usr/bin/env bash
#
# Profile a dataset with the productionized column profiler, then run lineage +
# classification for each profiled table. This is the by-dataset entry point:
# it profiles into the column-profiles table and immediately classifies, so the
# WRITE_TRUNCATE-per-partition profiler never clobbers another dataset's rows
# before they are consumed.
#
# Usage:
#   script/metadata/classify_dataset.sh <dataset> [<table> <table> ...]
#
#   <dataset>           bare dataset name in $SOURCE_PROJECT (e.g. ads_derived)
#   <table>...          optional subset of bare table names; default profiles
#                       every base table in the dataset
#
# Required env vars:
#   ANTHROPIC_API_KEY   for Claude models
#   DATAHUB_GMS_TOKEN   for lineage_probe_fetcher
# For Gemini models: `gcloud auth application-default login` (Vertex AI)
#
# Optional env vars (defaults shown):
#   MODELS=gemini-3.1-flash-lite-preview     space-separated model list
#   SOURCE_PROJECT=moz-fx-data-shared-prod   source data (read); stays prod
#   CLASSIFICATION_PROJECT=mozdata-nonprod   output project (e.g. akomar-sandbox-438914)
#   CLASSIFICATION_DATASET=analysis          output dataset (created if missing)
#   LOCATION=US                              BQ location for a newly created dataset
#   DEST_TABLE=akomar_column_profiles_v1     profiling table name within that dataset
#   DATE=<today, UTC>                        the profiled_at partition

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <dataset> [<table> ...]" >&2
    exit 1
fi

DATASET="$1"; shift
TABLES=("$@")

SOURCE_PROJECT="${SOURCE_PROJECT:-moz-fx-data-shared-prod}"
CLASSIFICATION_PROJECT="${CLASSIFICATION_PROJECT:-mozdata-nonprod}"
CLASSIFICATION_DATASET="${CLASSIFICATION_DATASET:-analysis}"
# Export so the python phases (field_classifier, lineage_probe_fetcher) read the
# same destination from their environment.
export CLASSIFICATION_PROJECT CLASSIFICATION_DATASET
DEST_PROJECT="$CLASSIFICATION_PROJECT"
DEST_DATASET="$CLASSIFICATION_DATASET"
DEST_TABLE="${DEST_TABLE:-akomar_column_profiles_v1}"
LOCATION="${LOCATION:-US}"
DATE="${DATE:-$(date -u +%F)}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Prefer the repo venv so callers need not activate it first; export so the
# classify_table.sh child uses the same interpreter.
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
    PYTHON=python
    [[ -x venv/bin/python ]] && PYTHON=venv/bin/python
fi
export PYTHON

PROFILER="sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py"
SCHEMA_JSON="$REPO_ROOT/script/metadata/classification/column_profiles_schema.json"
DEST_FQN="${DEST_PROJECT}.${DEST_DATASET}.${DEST_TABLE}"

banner() {
    printf '\n========================================================================\n'
    printf '== %s\n' "$1"
    printf '========================================================================\n'
}

# 0. Pre-create the output dataset if missing (bq mk --table and the python loads
#    both require the dataset to already exist).
if ! bq show "${DEST_PROJECT}:${DEST_DATASET}" >/dev/null 2>&1; then
    banner "creating dataset ${DEST_PROJECT}:${DEST_DATASET} (${LOCATION})"
    bq mk --dataset --location="$LOCATION" "${DEST_PROJECT}:${DEST_DATASET}"
fi

# 1. Pre-create the profiling table if missing. The profiler loads into a
#    table$YYYYMMDD partition decorator, which does NOT auto-create the table;
#    it must already exist with matching partitioning + clustering.
if ! bq show "${DEST_PROJECT}:${DEST_DATASET}.${DEST_TABLE}" >/dev/null 2>&1; then
    banner "creating ${DEST_FQN}"
    bq mk --table \
        --time_partitioning_field=profiled_at \
        --time_partitioning_type=DAY \
        --clustering_fields=source_dataset,source_table,column_name \
        --schema="$SCHEMA_JSON" \
        "${DEST_PROJECT}:${DEST_DATASET}.${DEST_TABLE}"
fi

# 2. Profile the dataset (or the named table subset) into the $DATE partition.
banner "[profile] ${DATASET} -> ${DEST_FQN}\$${DATE//-/}"
PROFILE_ARGS=(
    --date "$DATE"
    --source-project "$SOURCE_PROJECT"
    --source-datasets "$DATASET"
    --destination-project "$DEST_PROJECT"
    --destination-dataset "$DEST_DATASET"
    --destination-table "$DEST_TABLE"
)
if [[ ${#TABLES[@]} -gt 0 ]]; then
    PROFILE_ARGS+=(--tables "${TABLES[@]}")
fi
"$PYTHON" "$PROFILER" "${PROFILE_ARGS[@]}"

# 3. Enumerate the tables actually profiled in this run, then classify each.
banner "[classify] enumerating profiled tables in ${DATASET}"
PROFILED_CSV="$(
    bq --project_id="$DEST_PROJECT" --format=csv query --use_legacy_sql=false \
        "SELECT DISTINCT source_table
         FROM \`${DEST_FQN}\`
         WHERE source_dataset = '${DATASET}' AND DATE(profiled_at) = DATE('${DATE}')
         ORDER BY source_table"
)"
# Portable (bash 3.2 has no mapfile); skip the CSV header and any blank lines.
PROFILED=()
while IFS= read -r _t; do
    [[ -n "$_t" ]] && PROFILED+=("$_t")
done < <(printf '%s\n' "$PROFILED_CSV" | tail -n +2)

if [[ ${#PROFILED[@]} -eq 0 ]]; then
    echo "No profiled tables found for ${DATASET} on ${DATE}; nothing to classify." >&2
    exit 0
fi

echo "Classifying ${#PROFILED[@]} table(s): ${PROFILED[*]}"
for TABLE in "${PROFILED[@]}"; do
    script/metadata/classify_table.sh "${SOURCE_PROJECT}.${DATASET}.${TABLE}"
done

banner "DONE: ${DATASET} (${#PROFILED[@]} tables)"
