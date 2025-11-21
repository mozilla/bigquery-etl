CREATE TEMP FUNCTION safe_parse_timestamp(ts string) AS (
  COALESCE(
        -- full datetime with offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", ts),
        -- date + offset (no time)
    SAFE.PARSE_TIMESTAMP("%F%Ez", ts),
        -- datetime with space before offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", REGEXP_REPLACE(ts, r"(\+|\-)(\d{2}):(\d{2})", "\\1\\2\\3"))
  )
);

-- serp_events_with_client_info_cte
SELECT
  glean_client_id AS client_id,
  submission_date,
  `moz-fx-data-shared-prod.udf.normalize_search_engine`(
    search_engine
  ) AS serp_provider_id, -- this is engine
  CASE
    WHEN partner_code = ''
      THEN NULL
    ELSE partner_code
  END AS partner_code,
  sap_source AS serp_search_access_point,
  sample_id,
  profile_group_id,
  legacy_telemetry_client_id,
  normalized_country_code AS country,
  normalized_app_name AS app_version,
  channel,
  normalized_channel,
  locale,
  os,
  normalized_os,
  os_version,
  normalized_os_version,
  CASE
    WHEN mozfun.norm.os(os) = "Windows"
      THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
    ELSE CAST(mozfun.norm.truncate_version(os_version, "major") AS STRING)
  END AS os_version_major,
  CASE
    WHEN mozfun.norm.os(os) = "Windows"
      THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
    ELSE CAST(mozfun.norm.truncate_version(os_version, "minor") AS string)
  END AS os_version_minor,
  windows_build_number,
  distribution_id,
  UNIX_DATE(DATE(safe_parse_timestamp(first_run_date))) AS profile_creation_date,
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
  subsession_start_time,
  subsession_end_time,
  subsession_counter,
  experiments
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2`
WHERE
  submission_date
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- submission_date = @submission_date
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      client_id,
      submission_date,
      serp_provider_id,
      partner_code,
      serp_search_access_point
    ORDER BY
      event_timestamp DESC
  ) = 1
