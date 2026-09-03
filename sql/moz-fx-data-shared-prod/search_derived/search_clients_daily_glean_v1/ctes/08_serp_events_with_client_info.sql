-- the date portion of a client-local timestamp string. Every value it reads carries a trailing
-- offset, so the date is its first ten characters. Converting to UTC would shift it a day for
-- part of the world, and profile_age_in_days subtracts two of these, so both terms have to be on
-- the same calendar. Parsing no time also absorbs the shapes that carry no seconds component.
CREATE TEMP FUNCTION local_date_of(ts STRING) AS (
  SAFE.PARSE_DATE('%F', SUBSTR(ts, 1, 10))
);

-- serp_events_with_client_info_cte
-- the four grain keys arrive resolved from serp_base_cte, so nothing is re-derived here
SELECT
  client_id,
  submission_date,
  provider_id,
  partner_code,
  search_access_point,
  sample_id,
  profile_group_id,
  legacy_telemetry_client_id,
  normalized_country_code AS country,
  normalized_app_name,
  browser_version_info.version AS app_version,
  browser_version_info.major_version AS app_major_version,
  browser_version_info.minor_version AS app_minor_version,
  browser_version_info.patch_revision AS app_patch_revision,
  channel,
  normalized_channel,
  locale,
  os,
  normalized_os,
  os_version,
  normalized_os_version,
  -- os_version_major and os_version_minor are derived in final_cte, from the coalesced inputs
  windows_build_number,
  distribution_id,
  UNIX_DATE(local_date_of(first_run_date)) AS profile_creation_date,
  region_home_region,
  usage_is_default_browser,
  search_engine_default_display_name,
  search_engine_default_load_path,
  search_engine_default_partner_code,
  search_engine_default_provider_id,
  search_engine_default_submission_url,
  search_engine_default_overridden_by_third_party,
  search_engine_private_display_name,
  search_engine_private_load_path,
  search_engine_private_partner_code,
  search_engine_private_provider_id,
  search_engine_private_submission_url,
  search_engine_private_overridden_by_third_party,
  CAST(overridden_by_third_party AS boolean) AS overridden_by_third_party,
  subsession_start_time AS ping_start_time,
  subsession_end_time AS ping_end_time,
  subsession_counter AS ping_seq,
  experiments
FROM
  `search_derived.search_clients_daily_glean_v1.serp_base_cte`
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      client_id,
      submission_date,
      provider_id,
      partner_code,
      search_access_point
    ORDER BY
      event_timestamp DESC,
      impression_id -- deterministic tiebreaker (unique serp_events_v2 key) so serp passthroughs are reproducible on ties
  ) = 1
