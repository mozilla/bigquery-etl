#!/usr/bin/env bash
# Run the queries for generating assets

set -ex

cd "$(dirname "$0")/.."

python3 -m etl-graph query-logs query_log_edges --project moz-fx-data-shared-prod
python3 -m etl-graph query-logs query_log_nodes --project moz-fx-data-shared-prod
python3 -m etl-graph index
