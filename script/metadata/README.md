# Metadata scripts - data classification PoC

Profiles BigQuery columns and assigns each one a data-type label from Mozilla's
data taxonomy (`classification/Taxonomy overview - Data Types.csv`) via an LLM
classifier. PoC: run by dataset, eyeball the output, iterate.

## Pipeline

All outputs go to one configurable destination, `mozdata-nonprod.analysis` by
default (override with `CLASSIFICATION_PROJECT` / `CLASSIFICATION_DATASET`, see
[Run over a dataset](#run-over-a-dataset)). The tables:

1. **Profile** - the productionized column profiler
   (`sql/moz-fx-data-shared-prod/data_governance_metadata_derived/column_profiles_v1/query.py`,
   from bigquery-etl PR #9503), run in place against our own table.
   -> `akomar_column_profiles_v1` (per-column stats + `column_tier`; no descriptions)
2. **Lineage + probes** - `lineage_probe_fetcher.py` walks DataHub lineage to the
   source ping and fetches probe defs (Glean probes carry `data_sensitivity`).
   -> `akomar_metadata_phase2_table_pings_v1`, `akomar_metadata_phase2_ping_probes_v1`
3. **Classify** - `field_classifier.py` labels each column against the taxonomy.
   -> `akomar_field_classifications_v1` (`primary_label`, `secondary_labels`,
   `confidence`, `reasoning`, `needs_review`, `data_collection_category`, `model`)

## Setup

```bash
uv venv --python 3.11 venv                                   # requirements.txt is compiled for 3.11
uv pip install --python venv/bin/python -r requirements.txt
gcloud auth application-default login                        # Gemini (Vertex) + bq CLI
export DATAHUB_GMS_TOKEN=...                                 # lineage
export ANTHROPIC_API_KEY=...                                 # Claude models
python script/metadata/classification/build_taxonomy.py      # regenerate taxonomy.json
```

## Run over a dataset

`classify_dataset.sh` is the main entry point. It creates the output dataset and
profiling table if missing, profiles the dataset, then runs lineage + classify
for each profiled table.

```bash
# every base table in the dataset
script/metadata/classify_dataset.sh ads_derived

# a subset of tables within one dataset
script/metadata/classify_dataset.sh firefox_desktop_stable newtab_v1 quick_suggest_v1

# pick models (default: a single Gemini run)
MODELS="claude-sonnet-4-6 gemini-3.1-flash-lite-preview" \
    script/metadata/classify_dataset.sh ads_derived
```

Overridable via env (defaults shown): `SOURCE_PROJECT=moz-fx-data-shared-prod`
(source data, read), `CLASSIFICATION_PROJECT=mozdata-nonprod` and
`CLASSIFICATION_DATASET=analysis` (where all outputs are written and read back;
the dataset is created if missing, in `LOCATION=US`), `DEST_TABLE=akomar_column_profiles_v1`,
`DATE=<today UTC>`.

To write into a dev sandbox instead, point those two at it (all phases follow):

```bash
CLASSIFICATION_PROJECT=moz-fx-data-proto CLASSIFICATION_DATASET=data_classification \
    script/metadata/classify_dataset.sh ads_derived
```

Your credentials must be able to write there (direct BQ access, or impersonate
the sandbox service account). Note: the shared sandbox makes datasets
broadly readable, so do not use it for restricted-PII outputs.

Run datasets one at a time: each profiling run truncates its `profiled_at`
partition, so the by-dataset flow (profile then classify, then next) avoids
clobbering. See
[`classification/profiler_productionization_plan.md`](classification/profiler_productionization_plan.md)
for the partition/clobber details.

## Run over a single (already-profiled) table

`classify_table.sh` runs only lineage + classify, so the table must already be
profiled (present in `akomar_column_profiles_v1` in the configured dataset); it
warns and skips any table that isn't. Useful for re-classifying without
re-profiling (a new model, or after a taxonomy tweak). Set the same
`CLASSIFICATION_*` vars here as you used for profiling:

```bash
script/metadata/classify_table.sh moz-fx-data-shared-prod.ads_derived.ad_metrics_v1
```

## Inspect

```sql
-- adjust the project.dataset if you retargeted via CLASSIFICATION_*
SELECT column_name, model, primary_label, confidence, needs_review, reasoning
FROM `mozdata-nonprod.analysis.akomar_field_classifications_v1`
WHERE source_table = 'ad_metrics_v1'
ORDER BY column_name, model;
```

## Models

`--model` (or `$MODELS` for the wrappers) selects the LLM by prefix: `claude-*`
-> Anthropic API (`ANTHROPIC_API_KEY`), `gemini-*` -> Vertex AI on `mozdata`
(ADC). Default `claude-sonnet-4-6`. The idempotency key includes `model`, so the
same table can be classified by several models and all rows are kept.

```bash
# diff two models on a table (run both first)
python script/metadata/classification/compare_models.py --table "$TABLE"
```

## Export for Legal review

`classification/export_to_sheet.py` writes a CSV of classifications for paste
into a Google Sheet (the source-table list + model are hardcoded at the top;
edit for scope). `--stdout | pbcopy` to skip the file.

## Classifier design

For each profiled column (latest snapshot, excluding `undocumented`): fuzzy-match
the column name to source-ping probes (top 3), then prompt the LLM with the
column name, data type, the column's existing BigQuery description (read live
from the source table's `COLUMN_FIELD_PATHS`, if it has one), profiling stats
(null rate, distinct count, top/example values, plus a PII-suppressed flag when
the profiler set one), the matched probe
(name/description/`data_sensitivity`/`tags`), and the compacted taxonomy. The
model returns the labels above and a `data_collection_category` (technical /
interaction / web_activity / highly_sensitive, per Mozilla's [data collection
categories](https://wiki.mozilla.org/Data_Collection#Data_Collection_Categories)),
deferring to a Glean `data_sensitivity` unless observed content overrides it. The
existing description is often the strongest signal for non-Glean tables that have
no probe.

## Non-goals (PoC)

- No ground-truth eval (the FxA plan is where one would start).
- `metrics STRUCT` columns stay `undocumented` and are not classified.
- No writeback to `schema.yaml` / `global.yaml` / DataHub tags.
- No retries on LLM JSON parse failures (log and skip).

## Plans

- [`classification/profiler_productionization_plan.md`](classification/profiler_productionization_plan.md)
  - using the productionized profiler (PR #9503); code side implemented, remaining
  work is operational. Tracks PR #9557 (Phase 2 productionization) for later.
- [`classification/fxa_classification_plan.md`](classification/fxa_classification_plan.md)
  - classifying all FxA data: mixed Glean / non-Glean provenance, restricted-PII
  handling, first ground-truth eval.
