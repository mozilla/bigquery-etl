#!/bin/bash
# generate sql for checking into the repository and for testing the workflow

set -e

project=${PROJECT:-glam-fenix-dev}
skip_generate=${SKIP_GENERATE:-false}
skip_daily=${SKIP_DAILY:-false}
generate_only=${GENERATE_ONLY:-false}
# NOTE: there are three app_ids that we must look at for historical context. For
# the purpose of this script, it is sufficient to look only at what is currently
# "firefox desktop". We must have at least one table scalar/histogram tables for
# each of the referenced tables in the view. We'll keep all pings for
# firefox_desktop, and only the metrics ping for the others.
app_ids=(
    "firefox_desktop"
)
logical_app_id="firefox_desktop_glam_nightly"

dir="$(dirname "$0")/.."
sql_dir=$dir/../../sql/$project/glam_etl

if [[ $skip_generate == false ]]; then
    for app_id in "${app_ids[@]}"; do
        PRODUCT=$app_id STAGE=daily $dir/generate_glean_sql &
    done
    wait
    # remove tables to reduce noise of checked-in queries
    for app_id in "${app_ids[@]}"; do
        if [[ $app_id == "firefox_desktop" ]]; then
            continue
        fi
        for path in "${sql_dir}/${app_id}__clients"*; do
            if [[ $path == "${sql_dir}/${app_id}__clients"*metrics* ]]; then
                continue
            fi
            rm -r $path
        done
    done
    PRODUCT=$logical_app_id STAGE=incremental $dir/generate_glean_sql
fi

if [[ $generate_only != false ]]; then
    bqetl glam glean update-schemas
    exit
fi

if [[ $skip_daily == false ]]; then
    for app_id in "${app_ids[@]}"; do
        PRODUCT=$app_id STAGE=daily $dir/run_glam_sql
    done
fi
PRODUCT=$logical_app_id STAGE=incremental $dir/run_glam_sql
bqetl glam glean update-schemas
