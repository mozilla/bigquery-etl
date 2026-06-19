# Plan: consume the productionized profiler in the classification pipeline

**Goal.** Replace this PoC's home-grown Phase 1 (the former `field_profiler.py`,
now removed) with the productionized column profiler from
[bigquery-etl PR #9503](https://github.com/mozilla/bigquery-etl/pull/9503)
(DENG-11204, merged 2026-06-08), writing to **our own** nonprod table until the
rest of the pipeline is productionized. We want **classification only**, no
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
    raw stats, so we feed those into the prompt instead - strictly richer than
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
  three identity columns, and only in its no-`--table` path - `classify_table.sh`
  always passes `--table`, so the swap is invisible to it). Nothing else depends
  on the profiling schema.

- **The production profiler is self-contained**: imports only
  `google.cloud.bigquery` + stdlib (no `anthropic`). Destination is fully
  configurable via `--destination-project/-dataset/-table`. It adds PII
  suppression (`pii_suppressed` tier - column name matched a PII pattern, never
  scanned) and nested/array tiers (`nested_leaf`, `scalar_array`) beyond our
  `scalar`/`leaf`/`undocumented`. `profiled_at` is `DATE` (not `TIMESTAMP`), and
  there is **no** `pass1_description` column.

## Work items

### 1. Use the profiler in place (no vendoring)

`column_profiles_v1/query.py` is on the branch (main was merged in). Run it
directly from its `sql/` path - it is self-contained (only
`google.cloud.bigquery` + stdlib, no relative imports), and the destination is
driven entirely by `--destination-*` flags, so it needs no edits:

```bash
python sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py \
  --date <date> --source-datasets <one-dataset> --tables <t1> <t2> \
  --destination-project mozdata-nonprod --destination-dataset analysis \
  --destination-table akomar_column_profiles_v1
```

Do **not** copy it into `script/metadata/`. A vendored copy would drift from the
production version we just synced, and the integration requires no changes to the
profiler. If we ever do need to modify it (e.g. the FxA plan's wish to extend PII
suppression, or switching to `WRITE_APPEND`), revisit then - editing a file under
`sql/` that the production pipeline owns shows as a diff against main and risks
merge conflicts, so prefer handling such needs without forking it (e.g. restrict
the destination ACL rather than patch the suppression list).

### 2. Pre-create the destination table

A decorator load will **not** auto-create the table, so create it once with
matching partitioning and clustering (or the loads error). Since the profiler
stays under `sql/` (not importable as a module), use `bq mk` with an explicit
schema rather than importing `_COLUMN_PROFILES_SCHEMA`:

```bash
bq mk --table \
  --time_partitioning_field=profiled_at --time_partitioning_type=DAY \
  --clustering_fields=source_dataset,source_table,column_name \
  --schema='source_project:STRING,source_dataset:STRING,source_table:STRING,column_name:STRING,data_type:STRING,null_rate:FLOAT,distinct_count:INTEGER,is_high_cardinality:BOOLEAN,example_value:STRING,values:RECORD,column_tier:STRING,profiled_at:DATE' \
  mozdata-nonprod:analysis.akomar_column_profiles_v1
```

Note: `bq mk --schema` can't express the `values` RECORD's subfields
(`value:STRING`, `frequency:INTEGER`) inline. Either pass a JSON schema file (a
12-field array mirroring `_COLUMN_PROFILES_SCHEMA` in `query.py`), or create the
table with `client.create_table()` and an explicit `SchemaField` list copied from
`query.py`. The partitioning (`profiled_at`, DAY) and clustering
(`source_dataset, source_table, column_name`) must match exactly.

### 3. Run the profiler

The profiler is **dataset-scoped**: `--source-datasets X` profiles every base
table in X, and an optional `--tables a b` subset requires **exactly one**
`--source-datasets` entry. Each run does `WRITE_TRUNCATE` on the run's
`profiled_at` date partition, so within one date partition only the most recent
run survives.

**By-dataset flow (recommended).** Profile a dataset, classify it, move on.
`classify_dataset.sh` does this end-to-end: it pre-creates the column-profiles
table if missing (`bq mk` from `classification/column_profiles_schema.json`),
runs the profiler for the dataset, then runs lineage + classify per profiled
table:

```bash
# whole dataset
MODELS="claude-sonnet-4-6 gemini-3.1-flash-lite-preview" \
  script/metadata/classify_dataset.sh ads_derived

# or a table subset within the dataset
script/metadata/classify_dataset.sh ads_backend_stable interaction_v1
```

Because each dataset is profiled **and consumed** before the next runs, the
WRITE_TRUNCATE-per-partition behavior never clobbers data in flight: today's
partition holds the current dataset, and classifications accumulate in the
output table (`akomar_field_classifications_v1`, WRITE_APPEND). One date, one
table, no date juggling.

The **one rule**: don't batch-profile several datasets into the same
date+table and then classify afterward - later profiling runs would wipe the
earlier datasets' rows before the classifier reads them.

**Batch-coexist flow (only if you need many datasets' profiles in the table at
once** - e.g. one combined classify pass over tables spanning datasets**).**
Give each dataset run its own `--date` so the partitions coexist; the
classifier's latest-snapshot `QUALIFY` reads across them fine (datasets are
disjoint). This is the only case that needs distinct dates.

Use `--dry-run` (passed through to `query.py`) to confirm the work list first.

### 4. Adapt `field_classifier.py`  *(implemented)*

The only real code change (~40 lines, mostly `load_phase1` + the prompt):

- **Point `PHASE1_TABLE`** at
  `mozdata-nonprod.analysis.akomar_column_profiles_v1`.
- **Drop `AND pass1_description IS NOT NULL`** and stop unconditionally selecting
  that column (it does not exist in this schema -> would error). Make it optional:
  at startup, probe `INFORMATION_SCHEMA.COLUMNS` for `pass1_description` and only
  select it when present. (Satisfies "use descriptions if they exist, ignore if
  not.")
- **Add latest-snapshot dedup** - the table is weekly snapshots, so without this
  each column gets classified once per snapshot:
  ```sql
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_project, source_dataset, source_table, column_name
    ORDER BY profiled_at DESC) = 1
  ```
- **Handle the new tiers.** Keep excluding `undocumented`. **Keep
  `pii_suppressed`** - a column whose name matched a PII pattern is the
  highest-signal thing to classify (almost certainly a `user.*` identifier /
  highly_sensitive). Also keep `scalar_array` and `nested_leaf`.
- **Feed raw stats into the prompt** in place of the description line:
  `null_rate`, `distinct_count`, `is_high_cardinality`, `example_value` / top
  `values`, and a "PII-suppressed (name matched a PII pattern)" note when
  applicable. Include `pass1_description` too *if* it was present.

`lineage_probe_fetcher.py` also had its `PHASE1_TABLE` repointed (it reads only
the identity columns, in its no-`--table` discovery path). Orchestration now
splits cleanly: `classify_dataset.sh` profiles a dataset up front, then
`classify_table.sh` (trimmed to `[1/2] lineage` + `[2/2] classify`, profiling
step dropped) runs per table.

## Gotcha: cross-project billing

The production script creates its client with `project=--source-project` (our
sources, `moz-fx-data-shared-prod`), and that governs job execution/billing.
There is no separate destination-job-project flag. The load writes to
`mozdata-nonprod`. So the running credentials need **BQ jobUser on
`moz-fx-data-shared-prod`** + **dataEditor on `mozdata-nonprod.analysis`**. (The
removed `field_profiler.py` billed in `mozdata-nonprod`, so this is a different
job-project than before.)

## Future awareness: PR #9557 (Phase 2 productionization)

[PR #9557](https://github.com/mozilla/bigquery-etl/pull/9557) (DENG-11205, by
phil-lee70, **OPEN/unmerged** as of 2026-06-16) productionizes Phase 2 - this
branch's `lineage_probe_fetcher.py` - as two tables in
`data_governance_metadata_derived`: `lineage_mapping_v1` (table -> ping) and
`probe_definitions_v1` (probe defs per ping). The chain is
`column_profiles_v1 -> lineage_mapping_v1 -> probe_definitions_v1`;
`lineage_mapping_v1` reads its driving table list straight out of
`column_profiles_v1`.

**Blocker for adopting it in the classifier later:** `probe_definitions_v1` keeps
only `probe_name`/`description`/`type` and **drops `data_sensitivity`, `tags`,
and `send_in_pings`** - exactly the Glean signals the classifier leans on (it is
told to defer to a declared `data_sensitivity`). Adopting #9557's probe table
will require extending its schema + Glean fetch first. No action now; just don't
treat it as a drop-in for Phase 2.

## Net effort

Run the in-tree profiler as-is, one `bq mk` / `create_table`, six profiling runs,
and ~40 lines of edits to `field_classifier.py`. Descriptions disappear as a
dependency and the classifier gains richer raw-stat signal.
