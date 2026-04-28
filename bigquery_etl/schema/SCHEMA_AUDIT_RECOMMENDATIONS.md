# Base Schema Audit -- Recommendations for Human Review

Generated: 2026-04-15
Audit scope: bigquery_etl/schema/ -- 4 files (global.yaml, ads_derived.yaml, search_derived.yaml, firefox_desktop_derived.yaml)

Items below were NOT auto-applied because they require human judgment.

---

## 1. Fields to Consider Moving OUT of global.yaml

These fields are currently in global.yaml but may be too product-specific or narrow in scope:

| Field | Reason for Review |
|---|---|
| `context_id` | Only relevant to contextual services pings (top-sites, sponsored content). May belong in a `contextual_services_derived.yaml` or `app_newtab.yaml` instead. |
| `source_column` | Primarily used in ads/data-ingestion pipelines, not generic telemetry. Consider moving to `ads_derived.yaml`. |
| `source_report` | Primarily used in ads/data-ingestion pipelines, not generic telemetry. Consider moving to `ads_derived.yaml`. Its alias `source_file` conflicts with a canonical field in `ads_derived.yaml` (see Section 4). |
| `adjust_network` | Adjust attribution fields are mobile-specific. Consider grouping all `adjust_*` fields into a mobile-specific schema file if one is created. |

## 2. Naming Issues Requiring Source Verification

| Field | File | Issue |
|---|---|---|
| `creative_type` | `ads_derived.yaml` | The original description read "The name of the flight associated with an ad. Referred elsewhere as flight name" which does not match the field name `creative_type`. The description was updated to describe a creative type, but the original intent should be verified against the source table schema to confirm whether `creative_type` truly holds a creative type value or is misnamed. |

## 3. Field Misplacements Requiring Priority Analysis

| Field | Current File | Candidate File | Reason |
|---|---|---|---|
| `experiments` (RECORD) | (none) | `firefox_desktop_derived.yaml` or `global.yaml` | Appears in 11 firefox_desktop_derived tables and 16 telemetry_derived tables. A RECORD type needs sub-field verification before promotion. |
| `attribution` (RECORD) | (none) | `global.yaml` or `firefox_desktop_derived.yaml` | Appears in 4 datasets as a RECORD. Sub-field structure differs across datasets (desktop attribution has different nested fields than VPN/subscription attribution). Cannot safely promote without verifying sub-field consistency. |
| `is_sponsored` (BOOLEAN) | (none) | `app_newtab.yaml` or `firefox_desktop_derived.yaml` | Appears in 5 firefox_desktop_derived tables. Product-specific to newtab/ads context. Unclear whether it belongs in a newtab product schema or a desktop dataset schema. |
| `newtab_content_surface_id` | (none) | `app_newtab.yaml` | Appears in 5 firefox_desktop_derived tables. Newtab-specific field that would belong in a product-level `app_newtab.yaml` if one is created. |

## 4. Canonical/Alias Conflicts Requiring Team Confirmation

| Conflict | Files Involved | Details |
|---|---|---|
| `source_file` | `global.yaml`, `ads_derived.yaml` | In `global.yaml`, `source_file` is an alias of canonical field `source_report`. In `ads_derived.yaml`, `source_file` is a standalone canonical field with description "Full name of the report file used to ingest the data." These represent the same concept but the canonical direction differs. The team should decide: (a) keep `source_report` as canonical in global and remove `source_file` from ads_derived, or (b) keep `source_file` as canonical in ads_derived and update the alias in global. |
| `dau` | `global.yaml`, `ads_derived.yaml` | Both files define `dau` as a canonical field. The global version has a richer general description; the ads_derived version has an ads-specific description and an alias `total_active`. The ads_derived version was kept because it has dataset-specific nuance (ads-specific description and alias), but the team should confirm this is the intended pattern. |
| `profile_group_id` | `global.yaml`, `ads_derived.yaml` | Both files define this as a canonical field with slightly different descriptions. The global version says "not shared with other telemetry data"; the ads version says "allowing user-oriented correlation of data." Team should decide if the ads version should be removed in favor of the global definition. |
| `flight_name` | `ads_derived.yaml` (canonical + alias on `creative_type`) | `flight_name` is defined as a canonical field AND as an alias of `creative_type` in the same file. The alias was removed during the `creative_type` description fix, but `flight_name` as a standalone field should be confirmed as correct. |

## 5. Complex RECORD Types Requiring Documentation Verification

| Field | Datasets | Tables | Notes |
|---|---|---|---|
| `experiments` | telemetry_derived, firefox_desktop_derived, fenix_derived, firefox_ios_derived | 30+ | Experiment enrollment RECORD with nested sub-fields (branch, enrollment_id, etc.). Sub-field descriptions need verification against Experimenter/Nimbus documentation. |
| `attribution` | firefox_desktop_derived, mozilla_vpn_derived, subscription_platform_derived, telemetry_derived | 15+ | Attribution RECORD with nested sub-fields that differ across products (desktop has campaign/content/medium/source/experiment/variation; VPN/subscriptions have UTM fields). Cannot safely define a single RECORD structure. |
| `metadata` | fenix_derived, firefox_desktop_derived, firefox_ios_derived, subscription_platform_derived, telemetry_derived | 20+ | Glean ping metadata RECORD. Sub-fields are well-documented in Glean SDK docs but should be verified before adding to a base schema. |

## 6. Dataset Schema Files Needing Population

The following datasets have significant numbers of tables but no dedicated base schema file. Running `base-schema-audit` against each would identify promotion candidates:

| Dataset | Table Count | Priority | Notes |
|---|---|---|---|
| `telemetry_derived` | 265 | High | Largest dataset; fields like `city`, `experiments`, `attribution_ua`, `attribution_dlsource` appear in 15+ tables. Many fields overlap with firefox_desktop_derived. |
| `fenix_derived` | 38 | Medium | Mobile-specific fields like `install_source`, `adjust_campaign`, `adjust_creative`, `meta_attribution_app` appear in 4-5 tables. Could warrant a `fenix_derived.yaml`. |
| `firefox_ios_derived` | 38 | Medium | Similar mobile fields as fenix_derived. Consider a shared mobile schema or individual dataset files. |
| `firefox_accounts_derived` | 51 | Medium | Log-structured data with fields like `timestamp`, `severity`, `labels`, `user_id` in 19+ tables. Distinct from telemetry patterns. |
| `subscription_platform_derived` | 65 | Medium | Subscription/billing fields like `subscription_id`, `product_id`, `plan_id`, `customer_id` in 8-14 tables. |
| `mozilla_vpn_derived` | 38 | Low | Many fields overlap with subscription_platform_derived. Consider shared schema. |
| `contextual_services_derived` | (check) | Low | Fields like `form_factor`, `impression_count`, `click_count` in 3-5 tables. |

### App-Level Schema Files to Consider Creating

| File | Rationale |
|---|---|
| `app_newtab.yaml` | 15+ newtab-specific fields (`newtab_content_surface_id`, `pocket_enabled`, `topsites_enabled`, `newtab_search_enabled`, etc.) appear in 4-6 newtab tables across firefox_desktop_derived and telemetry_derived. Referenced in CLAUDE.md as an example. |
| `app_mobile.yaml` | Adjust attribution fields (`adjust_campaign`, `adjust_creative`, `adjust_ad_group`, `adjust_network`) plus mobile-specific fields (`install_source`, `meta_attribution_app`) appear across fenix_derived, firefox_ios_derived, and telemetry_derived. Could unify mobile-specific fields that don't belong in global. |

## Summary Table

| Recommendation | File(s) | Effort | Priority | Blocked on |
|---|---|---|---|---|
| Resolve `source_file` canonical/alias conflict | global.yaml, ads_derived.yaml | Small | High | Team confirmation on canonical direction |
| Verify `creative_type` description matches source | ads_derived.yaml | Small | High | Source table verification |
| Resolve `dau` cross-file duplicate | global.yaml, ads_derived.yaml | Small | Medium | Team confirmation on pattern |
| Resolve `profile_group_id` cross-file duplicate | global.yaml, ads_derived.yaml | Small | Medium | Team confirmation |
| Review global.yaml scope (context_id, source_*, adjust_*) | global.yaml | Medium | Medium | Team discussion on schema scope |
| Create `app_newtab.yaml` | New file | Medium | Medium | Newtab team confirmation on field list |
| Create `app_mobile.yaml` | New file | Medium | Medium | Mobile team confirmation on field list |
| Promote RECORD fields (experiments, attribution, metadata) | Multiple | Large | Low | Sub-field documentation verification |
| Create `fenix_derived.yaml` | New file | Medium | Low | base-schema-audit run |
| Create `firefox_ios_derived.yaml` | New file | Medium | Low | base-schema-audit run |
| Create `firefox_accounts_derived.yaml` | New file | Medium | Low | base-schema-audit run |
| Create `subscription_platform_derived.yaml` | New file | Medium | Low | base-schema-audit run |
| Create `telemetry_derived.yaml` | New file | Large | Low | Requires scoping which fields to include vs. leave table-specific |
