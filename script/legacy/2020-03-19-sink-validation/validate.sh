#!/bin/bash

script_dir="script/legacy/2020-03-19-sink-validation"
cd "$(git rev-parse --show-toplevel)"

$script_dir/create_stable_k8s_tables \
  2>&1 | tee $script_dir/create_stable_k8s_tables.log

python3 -W ignore script/copy_deduplicate \
  --project_id=moz-fx-data-shar-nonprod-efed \
  --only='*_live.*_k8s' \
  --except=telemetry_live.main_v4_k8s \
  --date=2020-03-19 \
  --parallelism=12 \
  2>&1 | tee $script_dir/copy_deduplicate.log

python3 -W ignore script/copy_deduplicate \
  --project_id=moz-fx-data-shar-nonprod-efed \
  --only=telemetry_live.main_v4_k8s \
  --date=2020-03-19 \
  --slices=100 \
  --parallelism=20 \
  2>&1 | tee $script_dir/copy_deduplicate_main_v4.log

$script_dir/compare \
  --except telemetry_stable.main_v4 telemetry_stable.crash_v4 \
  --date=2020-03-19 \
  --parallelism=10 \
  2>&1 | tee $script_dir/compare.log

$script_dir/compare \
  --only=telemetry_stable.main_v4 \
  --date=2020-03-19 \
  --parallelism=4 \
  2>&1 | tee $script_dir/compare_main_v4.log

$script_dir/compare \
  --only=telemetry_stable.crash_v4 \
  --date=2020-03-19 \
  --parallelism=2 \
  2>&1 | tee $script_dir/compare_crash_v4.log
