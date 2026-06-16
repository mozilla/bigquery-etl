# Metadata scripts - data classification PoC

Tooling that profiles BigQuery columns and assigns each one a data-type label
from Mozilla's data taxonomy (`classification/Taxonomy overview - Data
Types.csv`). It reuses a field-profiling + telemetry-lineage pipeline to feed an
LLM classifier.

**Scope**: PoC. Speed over correctness. Single-table runs, eyeball the output,
iterate.

## Pipeline

| Script | Phase | Output table (`mozdata-nonprod.analysis.*`) |
|---|---|---|
| `field_profiler.py` | **1.** Profile every column (null rate, distinct count, top values) and generate a pass-1 description from observed data. | `akomar_data_profiling_v1` |
| `lineage_probe_fetcher.py` | **2.** Walk DataHub upstream lineage to the source ping, then fetch probe definitions (Glean Dictionary or legacy stable-table schema). Captures `data_sensitivity`, `send_in_pings`, and `tags` for Glean probes. | `akomar_metadata_phase2_table_pings_v1`, `akomar_metadata_phase2_ping_probes_v1` |
| `description_reconciler.py` | **3.** *(original PoC, not used by the classifier)* Reconcile phase-1 data-driven descriptions with phase-2 probe intent. | `gkabbz_metadata_phase3_reconciled_v1` |
| `field_classifier.py` | **4.** Classify each column against the taxonomy: primary + secondary labels, confidence, reasoning, data collection category. Reads phases 1 and 2 directly. | `akomar_field_classifications_v1` |

All scripts accept `--table project.dataset.table` and are idempotent
(already-processed rows are skipped on re-run).

## Layout

```
script/metadata/
  field_profiler.py          - Phase 1: profile every column + pass1 description
  lineage_probe_fetcher.py   - Phase 2: resolve source ping + fetch probe defs
                                        (Glean probes include data_sensitivity,
                                         send_in_pings, tags)
  description_reconciler.py  - original Phase 3 for descriptions (unused by the classifier)
  field_classifier.py        - Phase 4: classify columns against the taxonomy
  classify_table.sh          - wrapper: runs phases 1->2->4 per table, per model
  classification/
    Taxonomy overview - Data Types.csv   - source of truth (from legal/privacy)
    build_taxonomy.py                    - CSV -> taxonomy.json
    taxonomy.json                        - preprocessed, what the classifier reads
    compare_models.py                    - diff Claude vs Gemini classifications
    export_to_sheet.py                   - export classifications as CSV for Google Sheets
```

## Output tables (`mozdata-nonprod.analysis`)

| Table | Written by | Contents |
|---|---|---|
| `akomar_data_profiling_v1` | `field_profiler.py` | One row per column: null rate, distinct count, top values, pass1 description |
| `akomar_metadata_phase2_table_pings_v1` | `lineage_probe_fetcher.py` | Table -> source ping mapping (via DataHub lineage) |
| `akomar_metadata_phase2_ping_probes_v1` | `lineage_probe_fetcher.py` | Probe defs per ping, incl. `data_sensitivity`, `send_in_pings`, `tags` for Glean |
| `akomar_field_classifications_v1` | `field_classifier.py` | Final classification: `primary_label`, `secondary_labels`, `confidence`, `reasoning`, `needs_review`, `data_collection_category` (technical / interaction / web_activity / highly_sensitive), `model` (full model name) |

## Setup (one-time)

```bash
pip install -r requirements.txt   # adds anthropic + google-genai to the venv
export DATAHUB_GMS_TOKEN=...
# Claude backend:
export ANTHROPIC_API_KEY=...
# Gemini backend - uses Vertex AI on the `mozdata` project:
gcloud auth application-default login
python script/metadata/classification/build_taxonomy.py   # regenerate taxonomy.json
```

## Usage

**Classify one or more tables end-to-end** with the wrapper:

```bash
script/metadata/classify_table.sh \
    moz-fx-data-shared-prod.search_derived.search_clients_daily_v8 \
    moz-fx-data-shared-prod.ads_backend_stable.interaction_v1
```

It runs three phases per table (profile -> lineage/probes -> classify) with each
model in `$MODELS`. Default is a single Gemini run
(`gemini-3.1-flash-lite-preview`). To also classify with Claude and diff the two
afterward:

```bash
MODELS="claude-sonnet-4-6 gemini-3.1-flash-lite-preview" \
    script/metadata/classify_table.sh "$TABLE"
python script/metadata/classification/compare_models.py --table "$TABLE"
```

**Or run the phases manually** (default model for `field_classifier.py` is
`claude-sonnet-4-6`):

```bash
TABLE=moz-fx-data-shared-prod.search_derived.search_clients_daily_v8
python script/metadata/field_profiler.py        --table "$TABLE"
python script/metadata/lineage_probe_fetcher.py --table "$TABLE"
python script/metadata/field_classifier.py      --table "$TABLE"
python script/metadata/field_classifier.py      --table "$TABLE" --model gemini-3.1-flash-lite-preview
python script/metadata/classification/compare_models.py --table "$TABLE"
```

**Inspect results:**

```sql
SELECT column_name, model, primary_label, confidence, needs_review, reasoning, data_sensitivity
FROM `mozdata-nonprod.analysis.akomar_field_classifications_v1`
WHERE source_table = 'search_clients_daily_v8'
ORDER BY column_name, model;
```

## Model selection

`field_classifier.py --model <full-model-name>` picks the LLM; the backend is
inferred from the name prefix:

- `claude-*` -> Anthropic API (e.g. `claude-sonnet-4-6`, `claude-opus-4-7`).
  Requires `ANTHROPIC_API_KEY`.
- `gemini-*` -> Vertex AI on project `mozdata` (e.g.
  `gemini-3.1-flash-lite-preview`). Requires application-default credentials.

Default is `claude-sonnet-4-6`; any other prefix is rejected. The destination
table has a `model` column storing the full model name, and the idempotency key
is `(project, dataset, table, column, model)`, so multiple models can be run on
the same table and all rows are kept (including version-to-version comparisons
within a family).

## Comparing models

After classifying a table with two models:

```bash
python script/metadata/classification/compare_models.py --table "$TABLE"
```

If the table has exactly two distinct `model` values they are auto-picked;
otherwise pass them explicitly with `--left` / `--right`. Prints:

- agreement rate on `primary_label`
- per-model confidence distribution (high/medium/low counts)
- side-by-side reasoning for every disagreement, including each model's matched
  probe

Add `--show-agreements` to also dump the agreed rows. Omit `--table` to compare
across all classified tables.

## Exporting for Legal review

`export_to_sheet.py` produces a CSV of classifications for manual paste into a
Google Sheet. The source-table list and model are hardcoded at the top of the
script; edit them for a different scope. (Writing directly to Sheets via the API
is blocked by Mozilla's Workspace policy on the gcloud OAuth client's Sheets
scope, so CSV-and-paste is the path of least resistance.)

```bash
python script/metadata/classification/export_to_sheet.py
# writes script/metadata/classification/classifications.csv

# or pipe straight to the macOS clipboard:
python script/metadata/classification/export_to_sheet.py --stdout | pbcopy
```

In Google Sheets: open a fresh sheet, click cell A1, paste; Sheets auto-splits
the CSV. Re-runs overwrite the file (gitignored), so just paste again to refresh.

Output columns: `dataset, table, column_name, category, category_simple,
data_collection_category, confidence, reasoning, needs_review`. `category_simple`
rolls the assigned `primary_label` up to the closest "Data type" entry from the
taxonomy (e.g. `user.behavior.search.term` -> `user.behavior.search`,
`user.unique_id.client_id` -> `user.unique_id`).

## Taxonomy preprocessing

`build_taxonomy.py` parses the CSV and normalizes it:

- Strips blank/header-only rows.
- Fixes typos: `user.behaviour.*` -> `user.behavior.*`,
  `personnel.demographic.Marital_status _orientation` ->
  `personnel.demographic.sexual_orientation`,
  `personnel.human_resouces.*` -> `personnel.human_resources.*`.
- Synthesizes top-level subject labels (`system`, `user`, `company`,
  `personnel`, `jobapplicants`, `other`) from CSV section headers.

Emits ~133 entries of `{label, parent, level, display_name, description,
examples}`, where `level` is one of `subject` / `data_type` / `subcategory`
(which CSV column the entry came from).

## Classifier design

For each profiled column:

1. Fuzzy-match the column name against probes from the source ping (top 3).
2. Build an LLM prompt with: column name, data type, null rate, pass1
   description, matched probe (name + description + `data_sensitivity` + `tags`),
   and the full `taxonomy.json` compacted (~6k tokens, fits easily).
3. The model returns JSON: `{primary_label, secondary_labels, confidence,
   reasoning, needs_review, data_collection_category}`.
   - `data_collection_category` is one of `technical` / `interaction` /
     `web_activity` / `highly_sensitive` (Mozilla's [4 data collection
     categories](https://wiki.mozilla.org/Data_Collection#Data_Collection_Categories),
     the same scale as Glean's `data_sensitivity`). Always emitted, including
     when no probe matched. The model is told to defer to a Glean-declared
     `data_sensitivity` unless the column's observed content overrides it.
4. Write to BQ.

## Explicit non-goals for the PoC

- No ground-truth eval set, no accuracy measurement.
- No Phase 3 description reconciliation reuse - classifier reads Phase 1 + Phase 2 directly.
- No Tier 3 (`REPEATED RECORD`, `metrics STRUCT`) handling.
- No writeback to `schema.yaml` / `global.yaml` / DataHub tags - output stays in BQ for review.
- No batching / parallelism beyond what the existing scripts already do.
- No retries on LLM JSON parse failures - log and skip.

## Plans / in progress

Forward-looking work (these supersede some of the design above as they land):

- [`classification/profiler_productionization_plan.md`](classification/profiler_productionization_plan.md)
  - replace Phase 1 with the productionized profiler from bigquery-etl PR #9503,
  feed raw profile stats into the classifier prompt, and make descriptions
  optional. Revisits the "no Tier 3" non-goal (the production profiler adds
  nested/array tiers).
- [`classification/fxa_classification_plan.md`](classification/fxa_classification_plan.md)
  - test classifying all FxA (Mozilla Accounts) data. Revisits the
  "no ground-truth eval" non-goal and raises restricted-PII handling.
