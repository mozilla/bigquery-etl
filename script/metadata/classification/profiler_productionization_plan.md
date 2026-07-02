# Plan: consume the productionized profiler in the classification pipeline

**Goal.** Replace this PoC's home-grown Phase 1 (`field_profiler.py`) with the
productionized column profiler from
[bigquery-etl PR #9503](https://github.com/mozilla/bigquery-etl/pull/9503)
(DENG-11204, merged 2026-06-08), writing to **our own** nonprod table until the
rest of the pipeline is productionized. We want **classification only** — no
description generation.

This plan covers Phase 1 only. Phases 2 (lineage/probes) and 4 (classifier) keep
running from this branch's scripts, with the small classifier edits in §4.

## Background / findings

These were established by reading both code bases and adversarially verifying the
two load-bearing claims (both **confirmed**):

- **The classifier's only profiling-derived prompt signal is `pass1_description`.**
  `field_classifier.load_phase1()` selects exactly `source_project,
  source_dataset, source_table, column_name, data_type, pass1_description`
  (`WHERE column_tier != 'undocumented' AND pass1_description IS NOT NULL`), and
  `build_classification_prompt()` interpolates only `table`, `column_name`,
  `data_type`, `pass1_description`, plus the Phase-2 probe block. `null_rate`,
  `distinct_count`, `is_high_cardinality`, `example_value`, and `values` are
  never selected and never reach the model.
  - Consequence: dropping descriptions removes the classifier's *only*
    data-driven signal unless we replace it. The production table carries all the
    raw stats, so we feed those into the prompt instead — strictly richer than
    the lossy description summary, and it removes the dependency on the
    description-generation LLM step entirely.

- **The production `query.py` does not create its destination table.**
  `save_profiles()` always loads into a partition decorator (`table$YYYYMMDD`)
  with `WRITE_TRUNCATE`. A decorator load into a non-existent table **fails**
  (`CREATE_IF_NEEDED` only auto-creates through a *plain* table reference, never
  through `$`). In production the table pre-exists because bqetl deploys it from
  `schema.yaml`; our nonprod table has no such deploy step, so we must pre-create
  it (§2).

- **Only two scripts read the Phase-1 table.** `field_classifier.py` (the
  classifier) and `lineage_probe_fetcher.py` (only a `SELECT DISTINCT` of the
  three identity columns, and only in its no-`--table` path — `classify_table.sh`
  always passes `--table`, so the swap is invisible to it). Nothing else depends
  on the profiling schema.

- **The production profiler is self-contained**: imports only
  `google.cloud.bigquery` + stdlib (no `anthropic`). Destination is fully
  configurable via `--destination-project/-dataset/-table`. It adds PII
  suppression (`pii_suppressed` tier — column name matched a PII pattern, never
  scanned) and nested/array tiers (`nested_leaf`, `scalar_array`) beyond our
  `scalar`/`leaf`/`undocumented`. `profiled_at` is `DATE` (not `TIMESTAMP`), and
  there is **no** `pass1_description` column.

## Work items

### 1. Vendor the script (it is not on this branch)

`column_profiles_v1/query.py` merged to `main` after this branch forked.

```bash
git fetch origin main
git show origin/main:sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py \
  > script/metadata/column_profiler.py
```

Vendoring a copy under `script/metadata/` keeps the PoC self-contained until the
full pipeline lands. (Alternatively, run it directly from a `main` checkout.)

### 2. Pre-create the destination table

A decorator load will **not** auto-create the table, so create it once using the
script's own schema constant to guarantee an exact match (partitioning and
clustering must match or the loads error):

```python
from google.cloud import bigquery
import column_profiler as cp  # the vendored script

client = bigquery.Client(project="mozdata-nonprod")
t = bigquery.Table("mozdata-nonprod.analysis.akomar_column_profiles_v1",
                   schema=cp._COLUMN_PROFILES_SCHEMA)
t.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY, field="profiled_at")
t.clustering_fields = ["source_dataset", "source_table", "column_name"]
client.create_table(t)
```

### 3. Run the profiler

Two constraints interact:

- `--tables` (subset) requires **exactly one** `--source-datasets` entry. The 9
  target tables span **6 datasets**, so they cannot be done in one subset run.
- Each run does `WRITE_TRUNCATE` on `table$<date>`, so two same-date runs into
  the same table **clobber** each other.

Recommended zero-edit path: **6 per-dataset runs, each into its own partition
date** of the one table. The datasets are disjoint, so no column collides across
partitions.

```bash
P=script/metadata/column_profiler.py
D=mozdata-nonprod; DS=analysis; T=akomar_column_profiles_v1
run() { python $P --destination-project $D --destination-dataset $DS --destination-table $T \
        --source-project moz-fx-data-shared-prod --source-datasets "$1" --tables "${@:3}" --date "$2"; }

run ads_backend_stable          2026-06-10 interaction_v1
run ads_derived                 2026-06-09 ad_metrics_v1
run firefox_desktop_stable      2026-06-08 newtab_v1 quick_suggest_v1
run firefox_desktop_derived     2026-06-07 newtab_clients_daily_v2
run search_terms_derived        2026-06-06 suggest_impression_sanitized_v3 adm_daily_aggregates_v1
run contextual_services_derived 2026-06-05 event_aggregates_suggest_v1 request_payload_suggest_v2
```

Use `--dry-run` first to confirm the work list.

Alternatives, if the synthetic dates are undesirable:
- **One whole-dataset run** (all 6 datasets, no `--tables`, single real date, one
  `WRITE_TRUNCATE`, no clobber) — but it profiles *every* table in those
  datasets; `firefox_desktop_stable` alone is dozens of large ping tables, so
  this is expensive. Not recommended.
- **One-line edit** to the vendored copy (`WRITE_TRUNCATE` -> `WRITE_APPEND` and
  drop the `$` decorator) so all 6 runs append to one real-date partition. Clean,
  but it's a fork of the production script.

### 4. Adapt `field_classifier.py`

The only real code change (~40 lines, mostly `load_phase1` + the prompt):

- **Point `PHASE1_TABLE`** at
  `mozdata-nonprod.analysis.akomar_column_profiles_v1`.
- **Drop `AND pass1_description IS NOT NULL`** and stop unconditionally selecting
  that column (it does not exist in this schema -> would error). Make it optional:
  at startup, probe `INFORMATION_SCHEMA.COLUMNS` for `pass1_description` and only
  select it when present. (Satisfies "use descriptions if they exist, ignore if
  not.")
- **Add latest-snapshot dedup** — the table is weekly snapshots, so without this
  each column gets classified once per snapshot:
  ```sql
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_project, source_dataset, source_table, column_name
    ORDER BY profiled_at DESC) = 1
  ```
- **Handle the new tiers.** Keep excluding `undocumented`. **Keep
  `pii_suppressed`** — a column whose name matched a PII pattern is the
  highest-signal thing to classify (almost certainly a `user.*` identifier /
  highly_sensitive). Also keep `scalar_array` and `nested_leaf`.
- **Feed raw stats into the prompt** in place of the description line:
  `null_rate`, `distinct_count`, `is_high_cardinality`, `example_value` / top
  `values`, and a "PII-suppressed (name matched a PII pattern)" note when
  applicable. Include `pass1_description` too *if* it was present.

Phases 2 and 4 are otherwise untouched. Orchestration splits into: profile up
front (§3) -> then per-table `lineage_probe_fetcher.py --table` +
`field_classifier.py --table` (a trimmed `classify_table.sh` that drops its
`[1/3]` profiling step).

## Gotcha: cross-project billing

The production script creates its client with `project=--source-project` (our
sources, `moz-fx-data-shared-prod`), and that governs job execution/billing —
there is no separate destination-job-project flag. The load writes to
`mozdata-nonprod`. So the running credentials need **BQ jobUser on
`moz-fx-data-shared-prod`** + **dataEditor on `mozdata-nonprod.analysis`**. This
is a different job-project than the current `field_profiler.py` (which bills in
`mozdata-nonprod`).

## Future awareness: PR #9557 (Phase 2 productionization)

[PR #9557](https://github.com/mozilla/bigquery-etl/pull/9557) (DENG-11205, by
phil-lee70, **OPEN/unmerged** as of 2026-06-16) productionizes Phase 2 — this
branch's `lineage_probe_fetcher.py` — as two tables in
`data_governance_metadata_derived`: `lineage_mapping_v1` (table -> ping) and
`probe_definitions_v1` (probe defs per ping). The chain is
`column_profiles_v1 -> lineage_mapping_v1 -> probe_definitions_v1`;
`lineage_mapping_v1` reads its driving table list straight out of
`column_profiles_v1`.

**Blocker for adopting it in the classifier later:** `probe_definitions_v1` keeps
only `probe_name`/`description`/`type` and **drops `data_sensitivity`, `tags`,
and `send_in_pings`** — exactly the Glean signals the classifier leans on (it is
told to defer to a declared `data_sensitivity`). Adopting #9557's probe table
will require extending its schema + Glean fetch first. No action now; just don't
treat it as a drop-in for Phase 2.

## Net effort

One vendored script, one `bq mk` / `create_table`, six profiling runs, and ~40
lines of edits to `field_classifier.py`. Descriptions disappear as a dependency
and the classifier gains richer raw-stat signal.
