CREATE TEMP FUNCTION safe_parse_timestamp(ts STRING) AS (
  COALESCE(
        -- full datetime with offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", ts),
        -- date + offset (no time)
    SAFE.PARSE_TIMESTAMP("%F%Ez", ts),
        -- datetime with space before offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", REGEXP_REPLACE(ts, r"(\+|\-)(\d{2}):(\d{2})", "\\1\\2\\3"))
  )
);

-- sap_events_with_client_info_cte
SELECT
  client_id AS client_id,
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN JSON_VALUE(event_extra.provider_id) = 'other'
      THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_name)
        )
    ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(JSON_VALUE(event_extra.provider_id))
  END AS normalized_engine, -- this is "engine" in v8
  -- partner_code is a grain key, so it must never be NULL: an absent JSON key and an
  -- empty string both become 'no_code' so equality joins match rather than silently missing
  COALESCE(NULLIF(JSON_VALUE(event_extra.partner_code), ''), 'no_code') AS partner_code,
  CASE
    WHEN JSON_VALUE(event_extra.source) = 'abouthome'
      THEN 'about_home'
    WHEN JSON_VALUE(event_extra.source) = 'newtab'
      THEN 'about_newtab'
    ELSE REPLACE(JSON_VALUE(event_extra.source), '-', '_')
  END AS source,
-- end as search_access_point, -- rename
  sample_id,
  profile_group_id,
  legacy_telemetry_client_id, -- adding this for now so people can join to it if needed
  normalized_country_code AS country,
  normalized_app_name AS app_version,
  client_info.app_channel AS channel,
  normalized_channel,
  client_info.locale,
  client_info.os,
  normalized_os,
  client_info.os_version,
  normalized_os_version,
  client_info.windows_build_number,
  client_info.distribution.name AS distribution_id,
  UNIX_DATE(DATE(safe_parse_timestamp(client_info.first_run_date))) AS profile_creation_date,
  CAST(JSON_VALUE(metrics.string.region_home_region, '$') AS string) AS region_home_region,
  CAST(
    JSON_VALUE(metrics.boolean.usage_is_default_browser, '$') AS boolean
  ) AS usage_is_default_browser,
  CAST(
    JSON_VALUE(metrics.string.search_engine_default_display_name, '$') AS string
  ) AS search_engine_default_display_name,
  CAST(
    JSON_VALUE(metrics.string.search_engine_default_load_path, '$') AS string
  ) AS search_engine_default_load_path,
  CAST(
    JSON_VALUE(metrics.string.search_engine_default_partner_code, '$') AS string
  ) AS search_engine_default_partner_code,
  CAST(
    JSON_VALUE(metrics.string.search_engine_default_provider_id, '$') AS string
  ) AS search_engine_default_provider_id,
  CAST(
    JSON_VALUE(metrics.url.search_engine_default_submission_url, '$') AS string
  ) AS search_engine_default_submission_url,
  CAST(
    JSON_VALUE(metrics.boolean.search_engine_default_overridden_by_third_party, '$') AS boolean
  ) AS search_engine_default_overridden_by_third_party,
  CAST(
    JSON_VALUE(metrics.string.search_engine_private_display_name, '$') AS string
  ) AS search_engine_private_display_name,
  CAST(
    JSON_VALUE(metrics.string.search_engine_private_load_path, '$') AS string
  ) AS search_engine_private_load_path,
  CAST(
    JSON_VALUE(metrics.string.search_engine_private_partner_code, '$') AS string
  ) AS search_engine_private_partner_code,
  CAST(
    JSON_VALUE(metrics.string.search_engine_private_provider_id, '$') AS string
  ) AS search_engine_private_provider_id,
  CAST(
    JSON_VALUE(metrics.url.search_engine_private_submission_url, '$') AS string
  ) AS search_engine_private_submission_url,
  CAST(
    JSON_VALUE(metrics.boolean.search_engine_private_overridden_by_third_party, '$') AS boolean
  ) AS search_engine_private_overridden_by_third_party,
  JSON_VALUE(event_extra.provider_name) AS provider_name,
  JSON_VALUE(event_extra.provider_id) AS provider_id,
  CAST(
    JSON_VALUE(event_extra.overridden_by_third_party, '$') AS boolean
  ) AS overridden_by_third_party,
  ping_info.start_time AS ping_start_time,
  ping_info.end_time AS ping_end_time,
  ping_info.seq AS ping_seq,
  [
    STRUCT(
      JSON_KEYS(experiments)[OFFSET(0)] AS key,
      STRUCT(
        REPLACE(
          TO_JSON_STRING(experiments[JSON_KEYS(experiments)[OFFSET(0)]].branch),
          '"',
          ''
        ) AS branch,
        STRUCT(
          REPLACE(
            TO_JSON_STRING(experiments[JSON_KEYS(experiments)[OFFSET(0)]].extra.type),
            '"',
            ''
          ) AS type,
          REPLACE(
            TO_JSON_STRING(experiments[JSON_KEYS(experiments)[OFFSET(0)]].extra.enrollment_id),
            '"',
            ''
          ) AS enrollment_id
        ) AS extra
      ) AS value
    )
  ] AS experiments
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event = 'sap.counts'
-- this is to get the last instance
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      client_id,
      submission_date,
      normalized_engine,
      partner_code,
      source
    ORDER BY
      event_timestamp DESC
  ) = 1
