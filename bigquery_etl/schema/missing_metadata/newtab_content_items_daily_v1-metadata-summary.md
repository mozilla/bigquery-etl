# Schema Enricher — Metadata Summary

**Table:** `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_content_items_daily_v1`
**Run date:** 2026-04-23

---

## Phase Results

| Phase (Step) | Status | Notes |
|---|---|---|
| Discovery (Step 0) | ✅ | global.yaml found on GitHub main; app_newtab.yaml → 404 (not on GitHub main); firefox_desktop_derived.yaml → 404 (not on GitHub main) |
| Categorization (Step 1) | ✅ | 4 covered by global.yaml, 12 retained from existing descriptions, 3 flagged for fill |
| Description Fill (Step 2) | ✅ | 4 from global.yaml (base schema); 2 from upstream source schema; 1 from query context |
| Column Validation (Step 3) | ⚠️ | bqetl schema update/validate requires GCP auth (not available); query SELECT verified manually — all 19 columns match |
| Quality Check (Step 4) | ✅ | 3 descriptions corrected (matches_selected_topic typo, section_position typo, content_redacted clarified); 19 descriptions pass |
| Write (Step 5) | ✅ | schema.yaml written with all 19 fields |
| Verification (Step 5) | ✅ | All 19 fields confirmed, all descriptions non-empty |

---

## Base Schema Coverage

| Column | Matched Field | Source File | Source Status | Alias Used |
|---|---|---|---|---|
| submission_date | submission_date | global.yaml | live | no |
| channel | normalized_channel | global.yaml | live | yes (alias: channel) |
| country | country | global.yaml | live | no |
| app_version | app_version | global.yaml | live | no |
| newtab_content_surface_id | — | (none) | non-base-schema | — |
| corpus_item_id | — | (none) | non-base-schema | — |
| newtab_content_ping_version | — | (none) | non-base-schema | — |
| position | — | (none) | own description retained | — |
| is_sponsored | — | (none) | own description retained | — |
| is_section_followed | — | (none) | own description retained | — |
| matches_selected_topic | — | (none) | own description corrected | — |
| received_rank | — | (none) | own description retained | — |
| section | — | (none) | own description retained | — |
| section_position | — | (none) | own description corrected | — |
| topic | — | (none) | own description retained | — |
| content_redacted | — | (none) | own description corrected | — |
| impression_count | — | (none) | own description retained | — |
| click_count | — | (none) | own description retained | — |
| dismiss_count | — | (none) | own description retained | — |

---

## Non-Base-Schema Descriptions

| Column | Description | Inference Basis | Recommended Base Schema |
|---|---|---|---|
| newtab_content_surface_id | The surface identifier for the newtab content ping. | upstream source schema (newtab_content_reported_content_v1/schema.yaml) | app_newtab.yaml |
| corpus_item_id | A content identifier. For organic Newtab recommendations it is an opaque id produced by Newtab's recommendation systems that corresponds uniquely to the URL. This is the replacement for tile_id and scheduled_corpus_item_id. | upstream source schema (newtab_items_daily_v1/schema.yaml) | app_newtab.yaml |
| newtab_content_ping_version | The version of the newtab_content ping schema used to send the event. Used to distinguish events originating from the dedicated Newtab Content ping versus the legacy newtab ping. | query context — sourced from metrics.quantity.newtab_content_ping_version; used as IS NOT NULL filter | app_newtab.yaml |

---

## Column Validation Results

GCP authentication was not available so `bqetl query schema update/validate` could not be run. Manual verification against query.sql SELECT output:

- **Columns added (missing from schema):** none
- **Columns removed (not in query):** none
- **Type corrections:** none — all 19 columns match query SELECT output

---

## Quality Fixes Applied

| Column | Issue | Fix Applied |
|---|---|---|
| matches_selected_topic | Typo: "if a the topic" | Corrected to "whether the topic" |
| section_position | Typo: "numberic" | Corrected to "numeric" |
| content_redacted | Ambiguous phrasing "Are content details sent separately" | Clarified to describe STRING value semantics and table filter context |

---

## Output Files

- `sql/moz-fx-data-shared-prod/firefox_desktop_derived/newtab_content_items_daily_v1/schema.yaml` — enriched with all 19 descriptions
- `bigquery_etl/schema/missing_metadata/newtab_content_items_daily_v1_missing_metadata.yaml` — 3 non-base-schema descriptions (priorities 4–5)
- `bigquery_etl/schema/missing_metadata/newtab_content_items_daily_v1-metadata-summary.md` — this summary file
