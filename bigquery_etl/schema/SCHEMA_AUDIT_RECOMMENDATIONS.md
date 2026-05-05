# Base Schema Audit — Recommendations for Human Review

Generated: 2026-04-21
Audit scope: `bigquery_etl/schema/` — 4 files | Missing metadata files processed: 8

Items below were NOT auto-applied because they require human judgment. Each bullet references the `_missing_metadata.yaml` file(s) that surfaced the issue so reviewers can trace evidence.

---

## 1. Fields to Consider Moving OUT of global.yaml

None surfaced in this run. Existing global.yaml fields are retained as-is (moving a field out of `global.yaml` can silently break any table relying on `--use-global-schema` for that field).

## 2. Naming Issues Requiring Source Verification

- **`normalized_os_version`** — referenced as a missing column in `clients_first_seen_v1_missing_metadata.yaml` (type STRING) and already present as an alias of `os_version` in `global.yaml`. The candidate's recommended_target was `global.yaml`. No action taken. Please confirm that the source table column is literally named `normalized_os_version` and that it should remain an alias rather than a canonical field. If it is a canonical (distinct) field, it must be promoted as-is rather than left as an alias.

## 3. Field Misplacements Requiring Priority Analysis

- **`legacy_telemetry_client_id`** — currently canonical in both `ads_derived.yaml` and flagged in `baseline_active_users_v1_missing_metadata.yaml` (local_only_base_schema_columns) and `cfs_ga4_attr_v1_missing_metadata.yaml` (recommended_target: `global.yaml`). Moving it out of `ads_derived.yaml` into `global.yaml` could change which description is returned for ads tables. Requires an owner decision — retain in ads_derived.yaml as a product-scoped alias, or promote to global.yaml (and remove from ads_derived.yaml).
- **`legacy_telemetry_profile_group_id`** — flagged in `cfs_ga4_attr_v1_missing_metadata.yaml` with recommended_target `global.yaml`. Not yet promoted because evidence is from only a single table/dataset; requires confirmation that this is a cross-dataset canonical field before adding to `global.yaml`.

## 4. Canonical/Alias Conflicts Requiring Team Confirmation

- **`flight_name`** — canonical in `ads_derived.yaml` AND listed as an alias of `creative_type` within the same `ads_derived.yaml`. These are incompatible. Please confirm: is `flight_name` its own canonical field, or is it an alias of `creative_type`? If both are valid (they are distinct columns in underlying tables), consider renaming or removing the alias.
- **`source_file`** — canonical in `ads_derived.yaml` AND aliased to `source_report` in `global.yaml`. A reader looking up descriptions for `source_file` would get different answers depending on lookup order. Recommend: remove the `source_file` alias from `global.yaml`'s `source_report` (since ads tables use source_file as a distinct canonical), or rename the ads canonical.
- **`os_version` type conflict** — previously `INTEGER` with description "E.g. 100.9.11" in `global.yaml`. Auto-corrected to `STRING` (type mismatch rule). Please verify that no table relies on the old INTEGER behavior.
- **`app_version` type conflict** — previously `INTEGER` with description "e.g. 1.0.3" in `global.yaml`. Auto-corrected to `STRING`. Please verify that no table relies on the old INTEGER behavior.

## 5. Complex RECORD Types Requiring Documentation Verification

The following RECORD / nested-RECORD fields were strongly recommended for `global.yaml` by multiple missing-metadata files, but were **not promoted** because sub-field descriptions need verification against external documentation (Glean / Nimbus / ingestion pipeline specs):

- **`attribution`** (RECORD, NULLABLE) — recommended for global.yaml by 5 missing-metadata files (`baseline_active_users_v1`, `cfs_ga4_attr_v1`, `clients_daily_joined_v1`, `clients_daily_v6`, `clients_first_seen_v1`, `clients_last_seen_v1`, `onboarding_v2`). Sub-fields vary by source: `attribution.campaign`, `attribution.content`, `attribution.medium`, `attribution.source`, `attribution.term` (Glean); plus `dlsource`, `dltoken`, `ua` on Firefox Desktop. Needs a single canonical RECORD schema + sub-field descriptions before promotion.
- **`experiments`** (RECORD, REPEATED) — recommended for global.yaml by 4 files (`baseline_active_users_v1`, `clients_daily_v6`, `clients_last_seen_v1`, `onboarding_v2`, `onboarding_hourly_v2`). Sub-fields: `experiments.key`, `experiments.value`, `experiments.value.branch`, `experiments.value.extra`, `experiments.value.extra.enrollment_id`, `experiments.value.extra.type`. Needs canonical Nimbus schema before promotion.
- **`metadata`** (RECORD, NULLABLE) — recommended by `onboarding_v2` and `onboarding_hourly_v2`. Sub-fields: `metadata.geo.city`, `metadata.header`, `metadata.user_agent.browser`, `metadata.user_agent.os`, `metadata.user_agent.version`. This is a standard Glean ingestion-pipeline RECORD; needs canonical schema.
- **`active_addons`** (RECORD, REPEATED) — promoted to `telemetry_derived.yaml` as the parent field, but its sub-fields (name, id, version, type, is_system, etc.) were not enumerated; add them once the upstream schema is confirmed.
- **`days_seen_in_experiment`** (RECORD, REPEATED) — flagged by `clients_last_seen_v1` for global.yaml. Sub-fields: `experiment`, `branch`, `bits`. Needs canonical schema.
- **`distribution`** (RECORD, NULLABLE) — flagged by `cfs_ga4_attr_v1`. Sub-field: `distribution.name`. Needs canonical schema before promotion.
- **`attribution_ext`, `distribution_ext`** (JSON) — flagged by `cfs_ga4_attr_v1` as global.yaml. JSON typed fields require domain review before canonical promotion.

## 6. recommended_target Conflicts Across Missing Metadata Files

The following fields had different `recommended_target` values across two or more `_missing_metadata.yaml` files. The agent did not auto-promote them and instead logged the conflict here.

| Field | File A → target | File B → target | Notes |
|---|---|---|---|
| `active_experiment_branch` | clients_daily_joined_v1 → global.yaml | clients_daily_v6 → telemetry_derived.yaml | Added to global.yaml with a deprecation note covering the telemetry_derived variant |
| `active_experiment_id` | clients_daily_joined_v1 → global.yaml | clients_daily_v6 → telemetry_derived.yaml | Added to global.yaml with a deprecation note |
| `active_hours_sum` | baseline_active_users_v1 → global.yaml | clients_daily_joined_v1, clients_daily_v6, clients_first_seen_v1, clients_last_seen_v1 → telemetry_derived.yaml | Promoted to telemetry_derived.yaml (4-of-5 vote). Confirm that baseline's definition matches telemetry_derived's. |
| `vendor` | clients_daily_v6 → global.yaml | clients_last_seen_v1 → telemetry_derived.yaml | Not promoted. Please confirm whether `vendor` is a cross-dataset dimension (global) or a telemetry ETL artifact. |
| `env_build_id`, `env_build_version` | clients_daily_v6 → telemetry_derived.yaml | clients_last_seen_v1 → global.yaml | Promoted to telemetry_derived.yaml (narrower scope wins on tie). Confirm this is correct. |
| `os_service_pack_major`, `os_service_pack_minor` | clients_daily_v6, clients_first_seen_v1 → telemetry_derived.yaml | clients_last_seen_v1 → global.yaml | Promoted to telemetry_derived.yaml (2-of-3 vote). |
| `install_year` | clients_daily_v6 → global.yaml | clients_first_seen_v1, clients_last_seen_v1 → telemetry_derived.yaml | Promoted to telemetry_derived.yaml (2-of-3 vote). |
| `update_channel` | clients_first_seen_v1 → telemetry_derived.yaml | clients_last_seen_v1 → global.yaml | Promoted to telemetry_derived.yaml. Confirm — this is distinct from `normalized_channel` in global.yaml. |
| `previous_build_id` | clients_daily_joined_v1 → telemetry_derived.yaml | clients_first_seen_v1 → global.yaml | Promoted to telemetry_derived.yaml (more specific). |
| `submission_timestamp_min` | clients_daily_joined_v1, clients_first_seen_v1 → global.yaml | clients_daily_v6, clients_last_seen_v1 → telemetry_derived.yaml | Promoted to telemetry_derived.yaml. This is an ETL aggregate, not a raw pipeline timestamp. Confirm. |
| `release_channel` | onboarding_v2 → global.yaml | onboarding_hourly_v2 → firefox_desktop_derived.yaml | Promoted to firefox_desktop_derived.yaml (Glean-specific sourcing). Note: related but not identical to `normalized_channel` in global.yaml. |

## 7. Single-Dataset Candidates Flagged for Review Before Global Promotion

These fields were recommended as `global.yaml` by only one `_missing_metadata.yaml` file (no cross-dataset confirmation). They were still promoted to global.yaml because the reasoning (e.g. "complement to app_version_major") was strong. Please verify they belong there:

- `app_build`, `app_version_patch_revision`, `app_version_is_major_release`, `distributor`, `durations`, `env_build_arch`, `geo_db_version`, `is_new_profile`, `partner_id` — from `baseline_active_users_v1` and/or `clients_daily_v6`.
- `days_had_8_active_ticks_bits`, `days_opened_dev_tools_bits`, `days_visited_10_uri_bits`, `days_visited_1_uri_normal_mode_bits`, `days_visited_1_uri_private_mode_bits`, `days_visited_5_uri_bits` — from `clients_last_seen_v1` only; confirm these are used across other Firefox Desktop last-seen tables before retaining in global.yaml.

## 8. Fields Not Promoted (Require Rewrite/Evidence)

The following categories of fields were surfaced by `clients_daily_v6_missing_metadata.yaml` and `clients_last_seen_v1_missing_metadata.yaml` as "approximately 280–330 additional fields omitted for brevity" — they are all tagged for `telemetry_derived.yaml` but the individual fields were never enumerated in the missing-metadata files:

- `histogram_parent_devtools_*_opened_count_sum` (~29 fields)
- `scalar_parent_urlbar_impression_autofill_*_sum` (~6 fields)
- `scalar_parent_urlbar_searchmode_*_sum` (~13 fields)
- `scalar_parent_os_environment_*` (~6 fields)
- `scalar_parent_urlbar_picked_*_sum` (~22 fields)
- `default_private_search_engine_*` (~5 fields)
- `search_content_*_sum`, `search_withads_*_sum`, `search_adclicks_*_sum` (~39 fields total)
- `user_pref_*` (~13 fields)
- `contextual_services_quicksuggest_*_sum` (~28 fields)
- `contextual_services_topsites_*` (~2 fields)
- `scalar_a11y_hcm_*`, `a11y_theme`, `text_recognition_*`, `places_*`, `scalar_parent_sidebar_*`, `scalar_parent_library_*`, `bookmark/history/logins_migrations_quantity_*`, `media_play_time_ms_*`, `scalar_parent_browser_engagement_*`, `scalar_parent_devtools_accessibility_*`, `scalar_parent_navigator_storage_*`, `scalar_parent_storage_sync_api_usage_*`, `scalar_parent_browser_ui_interaction_*`, `search_count_*` (many fields)

**Recommendation**: re-run `schema-enricher` against `clients_daily_v6` and `clients_last_seen_v1` with the enumeration expanded (no `# omitted for brevity` shortcut), then re-run this agent to fill these into `telemetry_derived.yaml` in a future pass.

## 9. Dataset Schema Files Needing Population

- `firefox_desktop_derived.yaml` — created in this run with 67 fields (GA4 attribution, stub attribution logs, Glean messaging-system fields). Please review for completeness.
- `telemetry_derived.yaml` — created in this run with 91 fields. Missing the ~280+ histogram/scalar/search/user_pref fields noted in Section 8.

---

## Summary Table

| Recommendation | File(s) | Effort | Priority | Blocked on |
|---|---|---|---|---|
| Confirm `os_version`/`app_version` INTEGER→STRING type fix is safe | `global.yaml` | Low | High | Spot-check downstream consumers |
| Resolve `flight_name` canonical/alias conflict | `ads_derived.yaml` | Low | High | Ad-tables owner |
| Resolve `source_file`/`source_report` alias conflict | `global.yaml`, `ads_derived.yaml` | Low | Medium | Ad-tables owner |
| Resolve `legacy_telemetry_client_id` placement | `ads_derived.yaml` vs `global.yaml` | Medium | Medium | Confirm usage across Firefox Desktop derived tables |
| Populate nested RECORD sub-fields (attribution, experiments, metadata, active_addons, days_seen_in_experiment, distribution) | `global.yaml`, `telemetry_derived.yaml`, `firefox_desktop_derived.yaml` | High | Medium | Canonical Glean/Nimbus/ingestion schema confirmation |
| Re-run schema-enricher on clients_daily_v6 and clients_last_seen_v1 without the "omitted for brevity" shortcut | `bigquery_etl/schema/missing_metadata/` | Medium | Medium | Requires agent re-invocation |
| Confirm the 11 recommended_target conflicts in Section 6 | Multiple | Low (per item) | Medium | Owning teams |
| Confirm single-dataset global.yaml candidates in Section 7 | `global.yaml` | Low (per item) | Low | Cross-team ping dictionary |
