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

CREATE TEMP FUNCTION to_utc_string(ts STRING) AS (
  FORMAT_TIMESTAMP(
    '%F %T UTC',  -- desired output format
    SAFE.PARSE_TIMESTAMP('%FT%H:%M:%E*S%Ez', ts),
    'UTC'
  )
);

WITH
-- list of ad blocking addons produced using this logic: https://github.com/mozilla/search-adhoc-analysis/tree/master/monetization-blocking-addons
adblocker_addons_cte AS (
  SELECT
    addon_id,
    addon_name
  FROM
    `moz-fx-data-shared-prod.revenue.monetization_blocking_addons`
  WHERE
    blocks_monetization
),
-- this table is the new glean adblocker addons metric ping (used to be legacy telemetry)
clients_with_adblocker_addons_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    TRUE AS has_adblocker_addon
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`,
    UNNEST(JSON_QUERY_ARRAY(metrics.object.addons_active_addons)) AS addons
  INNER JOIN
    adblocker_addons_cte
    ON adblocker_addons_cte.addon_id = JSON_VALUE(addons, '$.id')
  WHERE
    DATE(submission_timestamp)
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- date(submission_timestamp) = @submission_date
    AND NOT BOOL(JSON_QUERY(addons, '$.userDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.appDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.blocklisted'))
  GROUP BY
    client_id,
    DATE(submission_timestamp)
),
sap_is_enterprise_cte AS (
    -- we still need this because of document_id is not null
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    CAST(
      JSON_VALUE(
        array_last(ARRAY_AGG(metrics.boolean.policies_is_enterprise ORDER BY event_timestamp DESC)),
        '$'
      ) AS boolean
    ) AS policies_is_enterprise
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
  WHERE
    DATE(submission_timestamp)
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- date(submission_timestamp) = @submission_date
    AND event = 'sap.counts'
    AND document_id IS NOT NULL
  GROUP BY
    client_id,
    DATE(submission_timestamp)
),
sap_events_with_client_info_cte AS (
  SELECT
    client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN JSON_VALUE(event_extra.provider_id) = 'other'
        THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
            JSON_VALUE(event_extra.provider_name)
          )
      ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_id)
        )
    END AS normalized_engine, -- this is "engine" in v8
    CASE
      WHEN JSON_VALUE(event_extra.partner_code) = ''
        THEN NULL
      ELSE JSON_VALUE(event_extra.partner_code)
    END AS partner_code,
    CASE
      WHEN JSON_VALUE(event_extra.source) = 'urlbar-handoff'
        THEN 'urlbar_handoff'
      ELSE JSON_VALUE(event_extra.source)
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
    ping_info.start_time AS subsession_start_time,
    ping_info.end_time AS subsession_end_time,
    ping_info.seq AS subsession_counter,
    [
      STRUCT(
        json_keys(experiments)[OFFSET(0)] AS key,
        STRUCT(
          REPLACE(
            TO_JSON_STRING(experiments[json_keys(experiments)[OFFSET(0)]].branch),
            '"',
            ''
          ) AS branch,
          STRUCT(
            REPLACE(
              TO_JSON_STRING(experiments[json_keys(experiments)[OFFSET(0)]].extra.type),
              '"',
              ''
            ) AS type,
            REPLACE(
              TO_JSON_STRING(experiments[json_keys(experiments)[OFFSET(0)]].extra.enrollment_id),
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
    DATE(submission_timestamp)
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- date(submission_timestamp) = @submission_date
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
),
sap_events_clients_ad_enterprise_cte AS (
  SELECT
    sap_events_with_client_info_cte.client_id,
    sap_events_with_client_info_cte.submission_date,
    sap_events_with_client_info_cte.normalized_engine,
    sap_events_with_client_info_cte.partner_code,
    sap_events_with_client_info_cte.source,
    -- sap_events_with_client_info_cte.source as search_access_point, -- rename
    sap_events_with_client_info_cte.sample_id,
    sap_events_with_client_info_cte.profile_group_id,
    sap_events_with_client_info_cte.legacy_telemetry_client_id,
    sap_events_with_client_info_cte.country,
    sap_events_with_client_info_cte.app_version,
    sap_events_with_client_info_cte.channel,
    sap_events_with_client_info_cte.normalized_channel,
    sap_events_with_client_info_cte.locale,
    sap_events_with_client_info_cte.os,
    sap_events_with_client_info_cte.normalized_os,
    sap_events_with_client_info_cte.os_version,
    sap_events_with_client_info_cte.normalized_os_version,
    sap_events_with_client_info_cte.windows_build_number,
    sap_events_with_client_info_cte.distribution_id,
    sap_events_with_client_info_cte.profile_creation_date,
    sap_events_with_client_info_cte.region_home_region,
    sap_events_with_client_info_cte.usage_is_default_browser,
    sap_events_with_client_info_cte.search_engine_default_display_name,
    sap_events_with_client_info_cte.search_engine_default_load_path,
    sap_events_with_client_info_cte.search_engine_default_partner_code,
    sap_events_with_client_info_cte.search_engine_default_provider_id,
    sap_events_with_client_info_cte.search_engine_default_submission_url,
    sap_events_with_client_info_cte.search_engine_default_overridden_by_third_party,
    sap_events_with_client_info_cte.search_engine_private_display_name,
    sap_events_with_client_info_cte.search_engine_private_load_path,
    sap_events_with_client_info_cte.search_engine_private_partner_code,
    sap_events_with_client_info_cte.search_engine_private_provider_id,
    sap_events_with_client_info_cte.search_engine_private_submission_url,
    sap_events_with_client_info_cte.search_engine_private_overridden_by_third_party,
    sap_events_with_client_info_cte.provider_name,
    sap_events_with_client_info_cte.provider_id,
    sap_events_with_client_info_cte.overridden_by_third_party,
    sap_events_with_client_info_cte.subsession_start_time,
    sap_events_with_client_info_cte.subsession_end_time,
    sap_events_with_client_info_cte.subsession_counter,
    sap_events_with_client_info_cte.experiments,
    clients_with_adblocker_addons_cte.has_adblocker_addon,
    sap_is_enterprise_cte.policies_is_enterprise
  FROM
    sap_events_with_client_info_cte
  LEFT JOIN
    clients_with_adblocker_addons_cte
    USING (client_id, submission_date)
  LEFT JOIN
    sap_is_enterprise_cte
    USING (client_id, submission_date)
),
sap_aggregates_cte AS (
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN JSON_VALUE(event_extra.provider_id) = 'other'
        THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
            JSON_VALUE(event_extra.provider_name)
          )
      ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_id)
        )
    END AS normalized_engine,
    JSON_VALUE(event_extra.partner_code) AS partner_code,
    CASE
      WHEN JSON_VALUE(event_extra.source) = 'urlbar-handoff'
        THEN 'urlbar_handoff'
      ELSE JSON_VALUE(event_extra.source)
    END AS source,
    -- end as search_access_point, -- rename
    MAX(UNIX_DATE(DATE((ping_info.parsed_start_time)))) - MAX(
      UNIX_DATE(DATE(safe_parse_timestamp(client_info.first_run_date)))
    ) AS profile_age_in_days,
    COUNT(*) AS sap,
    COUNTIF(ping_info.seq = 1) AS sessions_started_on_this_day,
    SUM(
      CAST(JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_active_ticks') AS float64) / (
        3600 / 5
      )
    ) AS active_hours_sum,
    SUM(
      CAST(
        JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_tab_open_event_count') AS float64
      )
    ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(
      CAST(JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_uri_count') AS float64)
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    MAX(
      CAST(
        JSON_EXTRACT_SCALAR(
          metrics.quantity,
          '$.browser_engagement_max_concurrent_tab_count'
        ) AS float64
      )
    ) AS concurrent_tab_count_max
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
  WHERE
    DATE(submission_timestamp)
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- date(submission_timestamp) = @submission_date
    AND event = 'sap.counts'
  GROUP BY
    client_id,
    submission_date,
    normalized_engine,
    partner_code,
    source
    -- search_access_point
),
sap_final_cte AS (
  SELECT
    sap_events_clients_ad_enterprise_cte.client_id,
    sap_events_clients_ad_enterprise_cte.submission_date,
    sap_events_clients_ad_enterprise_cte.normalized_engine,
    sap_events_clients_ad_enterprise_cte.partner_code,
    sap_events_clients_ad_enterprise_cte.source,
    -- sap_events_clients_ad_enterprise_cte.source as search_access_point, -- rename
    sap_events_clients_ad_enterprise_cte.sample_id,
    sap_events_clients_ad_enterprise_cte.profile_group_id,
    sap_events_clients_ad_enterprise_cte.legacy_telemetry_client_id,
    sap_events_clients_ad_enterprise_cte.country,
    sap_events_clients_ad_enterprise_cte.app_version,
    sap_events_clients_ad_enterprise_cte.channel,
    sap_events_clients_ad_enterprise_cte.normalized_channel,
    sap_events_clients_ad_enterprise_cte.locale,
    sap_events_clients_ad_enterprise_cte.os,
    sap_events_clients_ad_enterprise_cte.normalized_os,
    sap_events_clients_ad_enterprise_cte.os_version,
    sap_events_clients_ad_enterprise_cte.normalized_os_version,
    sap_events_clients_ad_enterprise_cte.windows_build_number,
    sap_events_clients_ad_enterprise_cte.distribution_id,
    sap_events_clients_ad_enterprise_cte.profile_creation_date,
    sap_events_clients_ad_enterprise_cte.region_home_region,
    sap_events_clients_ad_enterprise_cte.usage_is_default_browser,
    sap_events_clients_ad_enterprise_cte.search_engine_default_display_name,
    sap_events_clients_ad_enterprise_cte.search_engine_default_load_path,
    sap_events_clients_ad_enterprise_cte.search_engine_default_partner_code,
    sap_events_clients_ad_enterprise_cte.search_engine_default_provider_id,
    sap_events_clients_ad_enterprise_cte.search_engine_default_submission_url,
    sap_events_clients_ad_enterprise_cte.search_engine_default_overridden_by_third_party,
    sap_events_clients_ad_enterprise_cte.search_engine_private_display_name,
    sap_events_clients_ad_enterprise_cte.search_engine_private_load_path,
    sap_events_clients_ad_enterprise_cte.search_engine_private_partner_code,
    sap_events_clients_ad_enterprise_cte.search_engine_private_provider_id,
    sap_events_clients_ad_enterprise_cte.search_engine_private_submission_url,
    sap_events_clients_ad_enterprise_cte.search_engine_private_overridden_by_third_party,
    sap_events_clients_ad_enterprise_cte.provider_name,
    sap_events_clients_ad_enterprise_cte.provider_id,
    sap_events_clients_ad_enterprise_cte.overridden_by_third_party,
    sap_events_clients_ad_enterprise_cte.subsession_start_time,
    sap_events_clients_ad_enterprise_cte.subsession_end_time,
    sap_events_clients_ad_enterprise_cte.subsession_counter,
    sap_events_clients_ad_enterprise_cte.experiments,
    sap_events_clients_ad_enterprise_cte.has_adblocker_addon,
    sap_events_clients_ad_enterprise_cte.policies_is_enterprise,
    sap_aggregates_cte.profile_age_in_days,
    sap_aggregates_cte.sap,
    sap_aggregates_cte.sessions_started_on_this_day,
    sap_aggregates_cte.active_hours_sum,
    sap_aggregates_cte.scalar_parent_browser_engagement_tab_open_event_count_sum,
    sap_aggregates_cte.scalar_parent_browser_engagement_total_uri_count_sum,
    sap_aggregates_cte.concurrent_tab_count_max
  FROM
    sap_events_clients_ad_enterprise_cte
  LEFT JOIN
    sap_aggregates_cte
    USING (client_id, submission_date, normalized_engine, partner_code, source)
    -- using(client_id, submission_date, normalized_engine, partner_code, search_access_point) -- rename
),
serp_is_enterprise_cte AS (
    -- we still need this because of document_id is not null
  SELECT
    glean_client_id AS client_id,
    submission_date,
    array_last(
      ARRAY_AGG(policies_is_enterprise ORDER BY event_timestamp DESC)
    ) AS policies_is_enterprise
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2`
  WHERE
    submission_date
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- submission_date = @submission_date
    AND document_id IS NOT NULL
  GROUP BY
    client_id,
    submission_date
),
serp_events_with_client_info_cte AS (
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
),
serp_events_clients_ad_enterprise_cte AS (
    -- serp_events_clients_ad_enterprise_cte
  SELECT
    serp_events_with_client_info_cte.client_id,
    serp_events_with_client_info_cte.submission_date,
    serp_events_with_client_info_cte.serp_provider_id,
    serp_events_with_client_info_cte.partner_code,
    serp_events_with_client_info_cte.serp_search_access_point,
    serp_events_with_client_info_cte.sample_id,
    serp_events_with_client_info_cte.profile_group_id,
    serp_events_with_client_info_cte.legacy_telemetry_client_id,
    serp_events_with_client_info_cte.country,
    serp_events_with_client_info_cte.app_version,
    serp_events_with_client_info_cte.channel,
    serp_events_with_client_info_cte.normalized_channel,
    serp_events_with_client_info_cte.locale,
    serp_events_with_client_info_cte.os,
    serp_events_with_client_info_cte.normalized_os,
    serp_events_with_client_info_cte.os_version,
    serp_events_with_client_info_cte.normalized_os_version,
    serp_events_with_client_info_cte.os_version_major,
    serp_events_with_client_info_cte.os_version_minor,
    serp_events_with_client_info_cte.windows_build_number,
    serp_events_with_client_info_cte.distribution_id,
    serp_events_with_client_info_cte.profile_creation_date,
    serp_events_with_client_info_cte.region_home_region,
    serp_events_with_client_info_cte.usage_is_default_browser,
    serp_events_with_client_info_cte.search_engine_default_display_name,
    serp_events_with_client_info_cte.search_engine_default_load_path,
    serp_events_with_client_info_cte.search_engine_default_partner_code,
    serp_events_with_client_info_cte.search_engine_default_provider_id,
    serp_events_with_client_info_cte.search_engine_default_submission_url,
    serp_events_with_client_info_cte.search_engine_default_overridden_by_third_party,
    serp_events_with_client_info_cte.search_engine_private_display_name,
    serp_events_with_client_info_cte.search_engine_private_load_path,
    serp_events_with_client_info_cte.search_engine_private_partner_code,
    serp_events_with_client_info_cte.search_engine_private_provider_id,
    serp_events_with_client_info_cte.search_engine_private_submission_url,
    serp_events_with_client_info_cte.search_engine_private_overridden_by_third_party,
    serp_events_with_client_info_cte.overridden_by_third_party,
    serp_events_with_client_info_cte.subsession_start_time,
    serp_events_with_client_info_cte.subsession_end_time,
    serp_events_with_client_info_cte.subsession_counter,
    serp_events_with_client_info_cte.experiments,
    clients_with_adblocker_addons_cte.has_adblocker_addon,
    serp_is_enterprise_cte.policies_is_enterprise
  FROM
    serp_events_with_client_info_cte
  LEFT JOIN
    clients_with_adblocker_addons_cte
    USING (client_id, submission_date)
  LEFT JOIN
    serp_is_enterprise_cte
    USING (client_id, submission_date)
),
serp_ad_click_target_cte AS (
  SELECT
    glean_client_id AS client_id,
    submission_date,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(
      search_engine
    ) AS serp_provider_id, -- this is engine
    partner_code,
    sap_source AS serp_search_access_point,
    STRING_AGG(DISTINCT ad_components.component, ', ') AS ad_click_target
  FROM
    `mozdata.firefox_desktop.serp_events`
  CROSS JOIN
    UNNEST(ad_components) AS ad_components
  WHERE
    submission_date
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- submission_date = @submission_date
  GROUP BY
    client_id,
    submission_date,
    serp_provider_id,
    partner_code,
    serp_search_access_point
),
serp_aggregates_cte AS (
  SELECT
    glean_client_id AS client_id,
    submission_date,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(
      search_engine
    ) AS serp_provider_id, -- this is engine
    partner_code,
    sap_source AS serp_search_access_point,
    LOGICAL_AND(ad_blocker_inferred) AS serp_ad_blocker_inferred,
    COUNTIF(
      (is_tagged IS TRUE)
      AND (
        sap_source = 'follow_on_from_refine_on_incontent_search'
        OR sap_source = 'follow_on_from_refine_on_serp'
      )
    ) AS serp_follow_on_searches_tagged_count,
    COUNTIF(is_tagged IS TRUE) AS serp_searches_tagged_count,
    COUNTIF(is_tagged IS TRUE AND num_ads_visible > 0) AS serp_with_ads_tagged_count,
    COUNTIF(is_tagged IS FALSE) AS serp_searches_organic_count,
    COUNTIF(is_tagged IS FALSE AND num_ads_visible > 0) AS serp_with_ads_organic_count,
    SUM(CASE WHEN is_tagged IS TRUE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_tagged_count,
    SUM(CASE WHEN is_tagged IS FALSE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_organic_count,
    SUM(num_ad_clicks) AS num_ad_clicks,
    SUM(num_non_ad_link_clicks) AS num_non_ad_link_clicks,
    SUM(num_other_engagements) AS num_other_engagements,
    SUM(num_ads_loaded) AS num_ads_loaded,
    SUM(num_ads_visible) AS num_ads_visible,
    SUM(num_ads_blocked) AS num_ads_blocked,
    SUM(num_ads_notshowing) AS num_ads_notshowing,
    MAX(UNIX_DATE(DATE(to_utc_string(subsession_start_time)))) - MAX(
      UNIX_DATE(DATE(safe_parse_timestamp(first_run_date)))
    ) AS profile_age_in_days,
    COUNT(*) AS serp_counts,
    COUNTIF(subsession_counter = 1) AS sessions_started_on_this_day,
    SUM(browser_engagement_active_ticks / (3600 / 5)) AS active_hours_sum,
    SUM(
      browser_engagement_tab_open_event_count
    ) AS serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(browser_engagement_uri_count) AS serp_scalar_parent_browser_engagement_total_uri_count_sum,
    MAX(browser_engagement_max_concurrent_tab_count) AS max_concurrent_tab_count_max
  FROM
    `mozdata.firefox_desktop.serp_events` -- serp_events_v2 doesn't have the aggregated fields like `num_ads_visible`
  WHERE
    submission_date
    BETWEEN '2025-06-25'
    AND '2025-09-25'
    AND sample_id = 0
    -- submission_date = @submission_date
  GROUP BY
    client_id,
    submission_date,
    serp_provider_id,
    partner_code,
    serp_search_access_point
),
serp_final_cte AS (
  SELECT
    serp_events_clients_ad_enterprise_cte.client_id,
    serp_events_clients_ad_enterprise_cte.submission_date,
    serp_events_clients_ad_enterprise_cte.serp_provider_id,
    serp_events_clients_ad_enterprise_cte.partner_code,
    serp_events_clients_ad_enterprise_cte.serp_search_access_point,
    serp_events_clients_ad_enterprise_cte.sample_id,
    serp_events_clients_ad_enterprise_cte.profile_group_id,
    serp_events_clients_ad_enterprise_cte.legacy_telemetry_client_id,
    serp_events_clients_ad_enterprise_cte.country,
    serp_events_clients_ad_enterprise_cte.app_version,
    serp_events_clients_ad_enterprise_cte.channel,
    serp_events_clients_ad_enterprise_cte.normalized_channel,
    serp_events_clients_ad_enterprise_cte.locale,
    serp_events_clients_ad_enterprise_cte.os,
    serp_events_clients_ad_enterprise_cte.normalized_os,
    serp_events_clients_ad_enterprise_cte.os_version,
    serp_events_clients_ad_enterprise_cte.normalized_os_version,
    serp_events_clients_ad_enterprise_cte.os_version_major,
    serp_events_clients_ad_enterprise_cte.os_version_minor,
    serp_events_clients_ad_enterprise_cte.windows_build_number,
    serp_events_clients_ad_enterprise_cte.distribution_id,
    serp_events_clients_ad_enterprise_cte.profile_creation_date,
    serp_events_clients_ad_enterprise_cte.region_home_region,
    serp_events_clients_ad_enterprise_cte.usage_is_default_browser,
    serp_events_clients_ad_enterprise_cte.search_engine_default_display_name,
    serp_events_clients_ad_enterprise_cte.search_engine_default_load_path,
    serp_events_clients_ad_enterprise_cte.search_engine_default_partner_code,
    serp_events_clients_ad_enterprise_cte.search_engine_default_provider_id,
    serp_events_clients_ad_enterprise_cte.search_engine_default_submission_url,
    serp_events_clients_ad_enterprise_cte.search_engine_default_overridden_by_third_party,
    serp_events_clients_ad_enterprise_cte.search_engine_private_display_name,
    serp_events_clients_ad_enterprise_cte.search_engine_private_load_path,
    serp_events_clients_ad_enterprise_cte.search_engine_private_partner_code,
    serp_events_clients_ad_enterprise_cte.search_engine_private_provider_id,
    serp_events_clients_ad_enterprise_cte.search_engine_private_submission_url,
    serp_events_clients_ad_enterprise_cte.search_engine_private_overridden_by_third_party,
    serp_events_clients_ad_enterprise_cte.overridden_by_third_party,
    serp_events_clients_ad_enterprise_cte.subsession_start_time,
    serp_events_clients_ad_enterprise_cte.subsession_end_time,
    serp_events_clients_ad_enterprise_cte.subsession_counter,
    serp_events_clients_ad_enterprise_cte.experiments,
    serp_events_clients_ad_enterprise_cte.has_adblocker_addon,
    serp_events_clients_ad_enterprise_cte.policies_is_enterprise,
    serp_ad_click_target_cte.ad_click_target,
    serp_aggregates_cte.serp_ad_blocker_inferred,
    serp_aggregates_cte.serp_follow_on_searches_tagged_count,
    serp_aggregates_cte.serp_searches_tagged_count,
    serp_aggregates_cte.serp_searches_organic_count,
    serp_aggregates_cte.serp_with_ads_organic_count,
    serp_aggregates_cte.serp_with_ads_tagged_count,
    serp_aggregates_cte.serp_ad_clicks_tagged_count,
    serp_aggregates_cte.serp_ad_clicks_organic_count,
    serp_aggregates_cte.num_ad_clicks,
    serp_aggregates_cte.num_non_ad_link_clicks,
    serp_aggregates_cte.num_other_engagements,
    serp_aggregates_cte.num_ads_loaded,
    serp_aggregates_cte.num_ads_visible,
    serp_aggregates_cte.num_ads_blocked,
    serp_aggregates_cte.num_ads_notshowing,
    serp_aggregates_cte.profile_age_in_days,
    serp_aggregates_cte.serp_counts,
    serp_aggregates_cte.sessions_started_on_this_day,
    serp_aggregates_cte.active_hours_sum,
    serp_aggregates_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
    serp_aggregates_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum,
    serp_aggregates_cte.max_concurrent_tab_count_max
  FROM
    serp_events_clients_ad_enterprise_cte
  LEFT JOIN
    serp_ad_click_target_cte
    USING (client_id, submission_date, serp_provider_id, partner_code, serp_search_access_point)
  LEFT JOIN
    serp_aggregates_cte
    USING (client_id, submission_date, serp_provider_id, partner_code, serp_search_access_point)
),
join_sap_serp_cte AS (
  SELECT
    sap_final_cte.client_id AS sap_client_id,
    serp_final_cte.client_id AS serp_client_id,
    sap_final_cte.submission_date AS sap_submission_date,
    serp_final_cte.submission_date AS serp_submission_date,
    sap_final_cte.normalized_engine AS sap_normalized_engine,
    serp_final_cte.serp_provider_id,
    sap_final_cte.partner_code AS sap_partner_code,
    serp_final_cte.partner_code AS serp_partner_code,
    sap_final_cte.source AS sap_search_access_point,
    serp_final_cte.serp_search_access_point,
    sap_final_cte.sample_id AS sap_sample_id,
    serp_final_cte.sample_id AS serp_sample_id,
    sap_final_cte.profile_group_id AS sap_profile_group_id,
    sap_final_cte.legacy_telemetry_client_id AS sap_legacy_telemetry_client_id,
    serp_final_cte.legacy_telemetry_client_id AS serp_legacy_telemetry_client_id,
    sap_final_cte.country AS sap_country,
    sap_final_cte.app_version AS sap_app_version,
    sap_final_cte.channel AS sap_channel,
    sap_final_cte.normalized_channel AS sap_normalized_channel,
    sap_final_cte.locale AS sap_locale,
    sap_final_cte.os AS sap_os,
    sap_final_cte.normalized_os AS sap_normalized_os,
    sap_final_cte.os_version AS sap_os_version,
    sap_final_cte.normalized_os_version AS sap_normalized_os_version,
    sap_final_cte.windows_build_number AS sap_windows_build_number,
    sap_final_cte.distribution_id AS sap_distribution_id,
    sap_final_cte.profile_creation_date AS sap_profile_creation_date,
    sap_final_cte.region_home_region AS sap_region_home_region,
    sap_final_cte.usage_is_default_browser AS sap_usage_is_default_browser,
    sap_final_cte.search_engine_default_display_name AS sap_default_search_engine_display_name,
    sap_final_cte.search_engine_default_load_path AS sap_default_search_engine_load_path,
    sap_final_cte.search_engine_default_partner_code AS sap_default_search_engine_partner_code,
    sap_final_cte.search_engine_default_provider_id AS sap_default_search_engine_provider_id,
    sap_final_cte.search_engine_default_submission_url AS sap_default_search_engine_submission_url,
    sap_final_cte.search_engine_default_overridden_by_third_party AS sap_default_search_engine_overridden,
    sap_final_cte.search_engine_private_display_name AS sap_default_private_search_engine_display_name,
    sap_final_cte.search_engine_private_load_path AS sap_default_private_search_engine_load_path,
    sap_final_cte.search_engine_private_partner_code AS sap_default_private_search_engine_partner_code,
    sap_final_cte.search_engine_private_provider_id AS sap_default_private_search_engine_provider_id,
    sap_final_cte.search_engine_private_submission_url AS sap_default_private_search_engine_submission_url,
    sap_final_cte.search_engine_private_overridden_by_third_party AS sap_default_private_search_engine_overridden,
    sap_final_cte.provider_name AS sap_provider_name,
    sap_final_cte.provider_id AS sap_provider_id,
    sap_final_cte.overridden_by_third_party AS sap_overridden_by_third_party,
    sap_final_cte.subsession_start_time AS sap_subsession_start_time,
    sap_final_cte.subsession_end_time AS sap_subsession_end_time,
    sap_final_cte.subsession_counter AS sap_subsession_counter,
    sap_final_cte.experiments AS sap_experiments,
    sap_final_cte.has_adblocker_addon AS sap_has_adblocker_addon,
    sap_final_cte.policies_is_enterprise AS sap_policies_is_enterprise,
    sap_final_cte.profile_age_in_days AS sap_profile_age_in_days,
    sap_final_cte.sap AS sap,
    sap_final_cte.sessions_started_on_this_day AS sap_sessions_started_on_this_day,
    sap_final_cte.active_hours_sum AS sap_active_hours_sum,
    sap_final_cte.scalar_parent_browser_engagement_tab_open_event_count_sum AS sap_scalar_parent_browser_engagement_tab_open_event_count_sum,
    sap_final_cte.scalar_parent_browser_engagement_total_uri_count_sum AS sap_scalar_parent_browser_engagement_total_uri_count_sum,
    sap_final_cte.concurrent_tab_count_max AS sap_concurrent_tab_count_max,
    serp_final_cte.profile_group_id AS serp_profile_group_id,
    serp_final_cte.country AS serp_country,
    serp_final_cte.app_version AS serp_app_version,
    serp_final_cte.channel AS serp_channel,
    serp_final_cte.normalized_channel AS serp_normalized_channel,
    serp_final_cte.locale AS serp_locale,
    serp_final_cte.os AS serp_os,
    serp_final_cte.normalized_os AS serp_normalized_os,
    serp_final_cte.os_version AS serp_os_version,
    serp_final_cte.normalized_os_version AS serp_normalized_os_version,
    serp_final_cte.os_version_major AS serp_os_version_major,
    serp_final_cte.os_version_minor AS serp_os_version_minor,
    serp_final_cte.windows_build_number AS serp_windows_build_number,
    serp_final_cte.distribution_id AS serp_distribution_id,
    serp_final_cte.profile_creation_date AS serp_profile_creation_date,
    serp_final_cte.region_home_region AS serp_region_home_region,
    serp_final_cte.usage_is_default_browser AS serp_usage_is_default_browser,
    serp_final_cte.search_engine_default_display_name AS serp_default_search_engine_display_name,
    serp_final_cte.search_engine_default_load_path AS serp_default_search_engine_load_path,
    serp_final_cte.search_engine_default_partner_code AS serp_default_search_engine_partner_code,
    serp_final_cte.search_engine_default_provider_id AS serp_default_search_engine_provider_id,
    serp_final_cte.search_engine_default_submission_url AS serp_default_search_engine_submission_url,
    serp_final_cte.search_engine_default_overridden_by_third_party AS serp_default_search_engine_overridden,
    serp_final_cte.search_engine_private_display_name AS serp_default_private_search_engine_display_name,
    serp_final_cte.search_engine_private_load_path AS serp_default_private_search_engine_load_path,
    serp_final_cte.search_engine_private_partner_code AS serp_default_private_search_engine_partner_code,
    serp_final_cte.search_engine_private_provider_id AS serp_default_private_search_engine_provider_id,
    serp_final_cte.search_engine_private_submission_url AS serp_default_private_search_engine_submission_url,
    serp_final_cte.search_engine_private_overridden_by_third_party AS serp_default_private_search_engine_overridden,
    serp_final_cte.overridden_by_third_party AS serp_overridden_by_third_party,
    serp_final_cte.subsession_start_time AS serp_subsession_start_time,
    serp_final_cte.subsession_end_time AS serp_subsession_end_time,
    serp_final_cte.subsession_counter AS serp_subsession_counter,
    serp_final_cte.experiments AS serp_experiments,
    serp_final_cte.has_adblocker_addon AS serp_has_adblocker_addon,
    serp_final_cte.policies_is_enterprise AS serp_policies_is_enterprise,
    serp_final_cte.ad_click_target AS serp_ad_click_target,
    serp_final_cte.serp_ad_blocker_inferred AS serp_ad_blocker_inferred,
    serp_final_cte.serp_follow_on_searches_tagged_count AS serp_follow_on_searches_tagged_count,
    serp_final_cte.serp_searches_tagged_count AS serp_searches_tagged_count,
    serp_final_cte.serp_searches_organic_count AS serp_searches_organic_count,
    serp_final_cte.serp_with_ads_organic_count AS serp_with_ads_organic_count,
    serp_final_cte.serp_with_ads_tagged_count AS serp_with_ads_tagged_count,
    serp_final_cte.serp_ad_clicks_tagged_count AS serp_ad_clicks_tagged_count,
    serp_final_cte.serp_ad_clicks_organic_count AS serp_ad_clicks_organic_count,
    serp_final_cte.num_ad_clicks AS serp_num_ad_clicks,
    serp_final_cte.num_non_ad_link_clicks AS serp_num_non_ad_link_clicks,
    serp_final_cte.num_other_engagements AS serp_num_other_engagements,
    serp_final_cte.num_ads_loaded AS serp_num_ads_loaded,
    serp_final_cte.num_ads_visible AS serp_num_ads_visible,
    serp_final_cte.num_ads_blocked AS serp_num_ads_blocked,
    serp_final_cte.num_ads_notshowing AS serp_num_ads_notshowing,
    serp_final_cte.profile_age_in_days AS serp_profile_age_in_days,
    serp_final_cte.serp_counts,
    serp_final_cte.sessions_started_on_this_day AS serp_sessions_started_on_this_day,
    serp_final_cte.active_hours_sum AS serp_active_hours_sum,
    serp_final_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum AS serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
    serp_final_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum AS serp_scalar_parent_browser_engagement_total_uri_count_sum,
    serp_final_cte.max_concurrent_tab_count_max AS serp_max_concurrent_tab_count_max
  FROM
    sap_final_cte
  FULL OUTER JOIN
    serp_final_cte
    ON (
      sap_final_cte.client_id = serp_final_cte.client_id
      OR (sap_final_cte.client_id IS NULL AND serp_final_cte.client_id IS NULL)
    )
    AND (
      sap_final_cte.submission_date = serp_final_cte.submission_date
      OR (sap_final_cte.submission_date IS NULL AND serp_final_cte.submission_date IS NULL)
    )
    AND (
      sap_final_cte.normalized_engine = serp_final_cte.serp_provider_id
      OR (sap_final_cte.normalized_engine IS NULL AND serp_final_cte.serp_provider_id IS NULL)
    )
    AND (
      sap_final_cte.partner_code = serp_final_cte.partner_code
      OR (sap_final_cte.partner_code IS NULL AND serp_final_cte.partner_code IS NULL)
    )
    AND (
      sap_final_cte.source = serp_final_cte.serp_search_access_point
      OR (sap_final_cte.source IS NULL AND serp_final_cte.serp_search_access_point IS NULL)
    )
),
consolidated AS (
  SELECT
    serp_submission_date AS submission_date,
    serp_client_id AS client_id,
    serp_legacy_telemetry_client_id AS legacy_telemetry_client_id, -- NEW
    serp_provider_id AS engine, -- this is normalized in serp_events_with_client_info
    serp_search_access_point AS source,
    serp_partner_code AS partner_code, -- NEW
    serp_country AS country,
    NULL AS addon_version,
    serp_app_version AS app_version,
    serp_windows_build_number AS windows_build_number, -- NEW
    serp_distribution_id AS distribution_id,
    serp_locale AS locale,
    serp_region_home_region AS user_pref_browser_search_region,
    NULL AS search_cohort,
    serp_os AS os,
    serp_normalized_os AS normalized_os, -- NEW
    serp_os_version AS os_version,
    serp_normalized_os_version AS normalized_os_version, -- NEW
    serp_channel AS channel,
    serp_normalized_channel AS normalized_channel, -- NEW
    serp_usage_is_default_browser AS is_default_browser,
    serp_profile_creation_date AS profile_creation_date,
    serp_default_search_engine_display_name AS default_search_engine,
    serp_default_search_engine_load_path AS default_search_engine_data_load_path,
    serp_default_search_engine_submission_url AS default_search_engine_data_submission_url,
    serp_default_search_engine_partner_code AS default_search_engine_partner_code, -- NEW
    serp_default_search_engine_provider_id AS default_search_engine_provider_id, -- NEW
    serp_default_search_engine_overridden AS default_search_engine_overridden, -- NEW
    serp_default_private_search_engine_display_name AS default_private_search_engine,
    serp_default_private_search_engine_load_path AS default_private_search_engine_data_load_path,
    serp_default_private_search_engine_submission_url AS default_private_search_engine_data_submission_url,
    serp_default_private_search_engine_partner_code AS default_private_search_engine_partner_code, -- NEW
    serp_default_private_search_engine_provider_id AS default_private_search_engine_provider_id, -- NEW
    serp_default_private_search_engine_overridden AS default_private_search_engine_overridden, -- NEW
    serp_sample_id AS sample_id,
    serp_subsession_start_time AS subsession_start_time, -- NEW
    serp_subsession_end_time AS subsession_end_time, -- NEW
    serp_subsession_counter AS subsession_counter, -- NEW
    NULL AS subsessions_hours_sum,
    serp_sessions_started_on_this_day AS sessions_started_on_this_day,
    serp_overridden_by_third_party AS overridden_by_third_party, -- NEW
    NULL AS active_addons_count_mean,
    serp_max_concurrent_tab_count_max AS max_concurrent_tab_count_max,
    serp_scalar_parent_browser_engagement_tab_open_event_count_sum AS tab_open_event_count_sum,
    serp_active_hours_sum AS active_hours_sum,
    serp_scalar_parent_browser_engagement_total_uri_count_sum AS total_uri_count,
    serp_experiments AS experiments,
    NULL AS scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
    NULL AS scalar_parent_urlbar_searchmode_handoff_sum,
    NULL AS scalar_parent_urlbar_searchmode_keywordoffer_sum,
    NULL AS scalar_parent_urlbar_searchmode_oneoff_sum,
    NULL AS scalar_parent_urlbar_searchmode_other_sum,
    NULL AS scalar_parent_urlbar_searchmode_shortcut_sum,
    NULL AS scalar_parent_urlbar_searchmode_tabmenu_sum,
    NULL AS scalar_parent_urlbar_searchmode_tabtosearch_sum,
    NULL AS scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
    NULL AS scalar_parent_urlbar_searchmode_topsites_newtab_sum,
    NULL AS scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
    NULL AS scalar_parent_urlbar_searchmode_touchbar_sum,
    NULL AS scalar_parent_urlbar_searchmode_typed_sum,
    serp_profile_age_in_days AS profile_age_in_days,
    serp_searches_organic_count AS organic,
    serp_searches_tagged_count AS tagged_sap,
    serp_follow_on_searches_tagged_count AS tagged_follow_on,
    sap,
    serp_counts,
    serp_ad_click_target AS ad_click,
    serp_ad_clicks_organic_count AS ad_click_organic,
    serp_with_ads_tagged_count AS search_with_ads,
    serp_with_ads_organic_count AS search_with_ads_organic,
    serp_ad_clicks_tagged_count AS ad_clicks_tagged,
    serp_ad_blocker_inferred AS ad_blocker_inferred,
    serp_num_ad_clicks AS num_ad_clicks, -- NEW
    serp_num_non_ad_link_clicks AS num_non_ad_link_clicks, -- NEW
    serp_num_other_engagements AS num_other_engagements, -- NEW
    serp_num_ads_loaded AS num_ads_loaded, -- NEW
    serp_num_ads_visible AS num_ads_visible, -- NEW
    serp_num_ads_blocked AS num_ads_blocked, -- NEW
    serp_num_ads_notshowing AS num_ads_notshowing, -- NEW
    NULL AS unknown,
    sap_normalized_engine AS normalized_engine,
    NULL AS is_sap_monetizable, -- REVISIT
    serp_has_adblocker_addon AS has_adblocker_addon,
    serp_policies_is_enterprise AS policies_is_enterprise,
    serp_os_version_major AS os_version_major,
    serp_os_version_minor AS os_version_minor,
    serp_profile_group_id AS profile_group_id
  FROM
    join_sap_serp_cte
),
final_cte AS (
  SELECT
    *
  FROM
    consolidated
)
SELECT
  *
FROM
  final_cte
