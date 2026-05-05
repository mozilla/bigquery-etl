# Schema Enrichment Summary: cfs_ga4_attr_v1

**Table:** `moz-fx-data-shared-prod.firefox_desktop_derived.cfs_ga4_attr_v1`
**Run date:** 2026-04-08

## Coverage Summary

| Category | Count |
|---|---|
| Total top-level fields | 88 |
| Covered by base schemas (global.yaml) | 15 |
| Filled from upstream source schemas (priority 4) | 13 |
| Filled from query context (priority 5) | 5 |
| Filled from application context (priority 6) | 55 |
| Total fields with descriptions | 88 |
| Fields missing descriptions | 0 |

## Base Schema Coverage (global.yaml) — 15 fields

| Column | Source | Notes |
|---|---|---|
| submission_date | global.yaml | exact match |
| first_seen_date | global.yaml | exact match |
| sample_id | global.yaml | exact match |
| client_id | global.yaml | exact match |
| country | global.yaml | exact match |
| distribution_id | global.yaml | exact match |
| locale | global.yaml | exact match |
| normalized_os | global.yaml | exact match |
| app_display_version | global.yaml | alias → app_version |
| normalized_channel | global.yaml | exact match |
| normalized_os_version | global.yaml | alias → os_version |
| isp | global.yaml | alias → isp_name |
| attribution_experiment | global.yaml | exact match |
| attribution_variation | global.yaml | exact match |
| is_desktop | global.yaml | exact match |

## Non-Base-Schema Descriptions — 73 fields

### Priority 4: Upstream Source Schemas — 13 fields

**Source: `stub_attribution_service_derived.dl_token_ga_attribution_lookup_v2`**
- `stub_attr_logs_first_seen_date` — from `first_seen_date`
- `stub_attr_logs_ga_client_id` — from `ga_client_id`
- `stub_attr_logs_stub_session_id` — from `stub_session_id`
- `stub_attr_logs_dl_token` — from `dl_token`
- `stub_attr_logs_download_source` — from `download_source`

**Source: `telemetry.ga4_sessions_firefoxcom_mozillaorg_combined`**
- `ga4_website` — from `flag`
- `ga4_ga_client_id` — from `ga_client_id`
- `ga4_session_date` — from `session_date`
- `ga4_session_start_timestamp` — from `session_start_timestamp`
- `ga4_had_download_event` — from `had_download_event`
- `ga4_firefox_desktop_downloads` — from `firefox_desktop_downloads`
- `ga4_last_reported_install_target` — from `last_reported_install_target`
- `ga4_all_reported_install_targets` — from `all_reported_install_targets`

### Priority 5: Query Context (query.sql derived columns) — 5 fields

- `attribution_dlsource` — `JSON_VALUE(cfs.attribution_ext.dlsource)`
- `attribution_dltoken` — `JSON_VALUE(cfs.attribution_ext.dltoken)`
- `attribution_ua` — `JSON_VALUE(cfs.attribution_ext.ua)`
- `windows_version` — `mozfun.norm.glean_windows_version_info(...)`
- `ga4_stub_session_id` — `ga4.stub_session_id` (join key between stub attribution logs and GA4)

### Priority 6: Application Context — 55 fields

Attribution record fields (6): `attribution`, `attribution.campaign`, `attribution.content`, `attribution.medium`, `attribution.source`, `attribution.term`

Distribution record fields (2): `distribution`, `distribution.name`

Extended JSON fields (2): `attribution_ext`, `distribution_ext`

Legacy telemetry identifiers (2): `legacy_telemetry_client_id`, `legacy_telemetry_profile_group_id`

OS fields (1): `windows_build_number`

GA4 geographic fields (3): `ga4_country`, `ga4_region`, `ga4_city`

GA4 device fields (5): `ga4_device_category`, `ga4_mobile_device_model`, `ga4_mobile_device_string`, `ga4_operating_system`, `ga4_operating_system_version`

GA4 browser/language fields (3): `ga4_language`, `ga4_browser`, `ga4_browser_version`

GA4 UTM event param fields (10): `ga4_first_campaign_from_event_params`, `ga4_distinct_campaigns_from_event_params`, `ga4_first_source_from_event_params`, `ga4_distinct_sources_from_event_params`, `ga4_first_medium_from_event_params`, `ga4_distinct_mediums_from_event_params`, `ga4_first_content_from_event_params`, `ga4_distinct_contents_from_event_params`, `ga4_first_term_from_event_params`, `ga4_distinct_terms_from_event_params`

GA4 experiment event param fields (4): `ga4_first_experiment_id_from_event_params`, `ga4_distinct_experiment_ids_from_event_params`, `ga4_first_experiment_branch_from_event_params`, `ga4_distinct_experiment_branches_from_event_params`

GA4 Google Ads event param fields (2): `ga4_first_gad_campaignid_from_event_params`, `ga4_distinct_gad_campaignid_from_event_params`

GA4 manual campaign fields (6): `ga4_manual_campaign_id`, `ga4_manual_campaign_name`, `ga4_manual_source`, `ga4_manual_medium`, `ga4_manual_term`, `ga4_manual_content`

GA4 Google Ads click fields (2): `ga4_gclid`, `ga4_gclid_array`

GA4 landing/install fields (2): `ga4_stub_session_id` (counted in priority 5), `ga4_landing_screen`

GA4 Google Ads campaign fields (5): `ga4_ad_google_campaign`, `ga4_ad_google_campaign_id`, `ga4_ad_google_group`, `ga4_ad_google_group_id`, `ga4_ad_google_account`

GA4 cross-channel attribution fields (6): `ga4_ad_crosschannel_source`, `ga4_ad_crosschannel_medium`, `ga4_ad_crosschannel_campaign`, `ga4_ad_crosschannel_campaign_id`, `ga4_ad_crosschannel_source_platform`, `ga4_ad_crosschannel_primary_channel_group`, `ga4_ad_crosschannel_default_channel_group`

## Validation

- Schema column count: 88 top-level fields
- All fields have non-empty descriptions
- All field types match query output types
- No extra columns detected; no missing columns detected
