# Schema Enricher — Metadata Summary

**Table:** `moz-fx-data-shared-prod.customer_experience_derived.knowledgebase_retrieval_index_v1`
**Run date:** 2026-05-13

---

## Phase Results

| Phase (Step) | Status | Notes |
|---|---|---|
| Discovery (Step 0) | ✅ | Base schemas fetched live from GitHub via column-description-finder |
| Categorization (Step 1) | ✅ | 1 covered by base, 1 stale own-description removed, 31 needed fill |
| Description Fill (Step 2) | ✅ | 1 from base schema (global.yaml), 31 from query/application context |
| Column Validation (Step 3) | ✅ | All 32 top-level + 8 nested fields match query output |
| Quality Check (Step 4) | ✅ | All 40 descriptions pass |
| Write (Step 5) | ✅ | schema.yaml written and confirmed |

---

## Base Schema Coverage

| Column | Matched Field | Source File | Source Status | Alias Used |
|---|---|---|---|---|
| locale | locale | global.yaml | live | no |
| (all others) | — | (none) | non-base-schema | — |

---

## Non-Base-Schema Descriptions

See `knowledgebase_retrieval_index_v1_missing_metadata.yaml` for the full list of 31 columns and recommended base-schema targets. All recommendations point to a future `customer_experience_derived.yaml` dataset schema (does not exist yet).

---

## Column Validation Results

- **Columns added (missing from schema):** none — all query output is described
- **Columns removed (not in query):** `creation_date` (was a stale stub copied from kitsune)
- **Type corrections:** none

---

## Output Files

- `sql/moz-fx-data-shared-prod/customer_experience_derived/knowledgebase_retrieval_index_v1/schema.yaml` — enriched with all 40 descriptions
- `bigquery_etl/schema/missing_metadata/knowledgebase_retrieval_index_v1_missing_metadata.yaml` — 31 non-base-schema descriptions, all KB- or pipeline-specific
- `bigquery_etl/schema/missing_metadata/knowledgebase_retrieval_index_v1-metadata-summary.md` — this summary file
