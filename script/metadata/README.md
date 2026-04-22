# Metadata scripts

Tooling for enriching BigQuery column metadata — descriptions and data-type
classifications — from table samples, telemetry lineage, and LLM calls.

## Pipeline

| Script | Phase | Output table (`mozdata-nonprod.analysis.*`) |
|---|---|---|
| `field_profiler.py` | **1.** Profile every column (null rate, distinct count, top values) and generate a pass-1 description from observed data. | `akomar_data_profiling_v1` |
| `lineage_probe_fetcher.py` | **2.** Walk DataHub upstream lineage to the source ping, then fetch probe definitions (Glean Dictionary or legacy stable-table schema). Captures `data_sensitivity`, `send_in_pings`, and `tags` for Glean probes. | `akomar_metadata_phase2_table_pings_v1`, `akomar_metadata_phase2_ping_probes_v1` |
| `description_reconciler.py` | **3.** *(original PoC, not used by the classifier)* Reconcile phase-1 data-driven descriptions with phase-2 probe intent, emit a final description. | `gkabbz_metadata_phase3_reconciled_v1` |
| `field_classifier.py` | **4.** Classify each column against Mozilla's data taxonomy — primary + secondary labels, confidence, reasoning. Reads phases 1 & 2 directly. | `akomar_field_classifications_v1` |

All scripts accept `--table project.dataset.table` and are idempotent
(already-processed rows are skipped on re-run).

## Data classification

The classification PoC lives in [`classification/`](classification/) — it
holds the taxonomy source CSV, preprocessed `taxonomy.json`, and the
`build_taxonomy.py` tool that maintains them. See
[`classification/PLAN.md`](classification/PLAN.md) for design, usage, and
non-goals.

**One-liner to classify a table end-to-end:**
```bash
TABLE=moz-fx-data-shared-prod.search_derived.search_clients_daily_v8
python script/metadata/field_profiler.py        --table "$TABLE"
python script/metadata/lineage_probe_fetcher.py --table "$TABLE"
python script/metadata/field_classifier.py      --table "$TABLE"
```

Requires `ANTHROPIC_API_KEY` and `DATAHUB_GMS_TOKEN` env vars and
`pip install anthropic`.
