-- Improved timestamp parsing function with better performance
CREATE TEMP FUNCTION safe_parse_timestamp(ts STRING) AS (
  COALESCE(
    SAFE.PARSE_TIMESTAMP("%FT%T%Ez", ts),
    SAFE.PARSE_TIMESTAMP("%F%Ez", ts),
    SAFE.PARSE_TIMESTAMP("%FT%T*E%Ez", ts)
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
    adblocker_addons_cte -- it's an inner join in https://github.com/mozilla/bigquery-etl/blob/main/sql/moz-fx-data-shared-prod/search_derived/search_clients_daily_v8/query.sql
    -- left join adblocker_addons_cte
    ON adblocker_addons_cte.addon_id = JSON_VALUE(addons, '$.id')
  WHERE
    -- date(submission_timestamp) = @submission_date
    -- this is for test runs
    DATE(submission_timestamp) = '2025-06-07'
    -- and sample_id = 0
    AND NOT BOOL(JSON_QUERY(addons, '$.userDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.appDisabled'))
    AND NOT BOOL(JSON_QUERY(addons, '$.blocklisted'))
  GROUP BY
    client_id,
    DATE(submission_timestamp)
),
-- we still need this because of document_id is not null
is_enterprise_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    mozfun.stats.mode_last(
      ARRAY_AGG(metrics.boolean.policies_is_enterprise ORDER BY submission_timestamp)
    ) AS policies_is_enterprise
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    -- date(submission_timestamp) = @submission_date
    -- this is for test runs
    DATE(submission_timestamp) = '2025-06-07'
    -- and sample_id = 0
    AND document_id IS NOT NULL
  GROUP BY
    client_id,
    DATE(submission_timestamp)
),
-- this cte gets client_info and sap counts from the sap.counts events ping and aggregates at client_id, day
sap_clients_events_cte AS (
  SELECT
    -- distinct
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id,
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
      WHEN JSON_VALUE(event_extra.source) = 'urlbar-handoff'
        THEN 'urlbar_handoff'
      ELSE JSON_VALUE(event_extra.source)
    END AS source,
    -- end as search_access_point, -- rename
    normalized_country_code AS country,
    normalized_app_name AS app_version,
    client_info.app_channel AS channel,
    ping_info.parsed_start_time AS subsession_start_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    normalized_os_version,
    UNIX_DATE(DATE(safe_parse_timestamp(client_info.first_run_date))) AS profile_creation_date,
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
    ] AS experiments,
    normalized_channel,
    normalized_os,
    sample_id,
    profile_group_id,
    legacy_telemetry_client_id, -- adding this for now so people can join to it if needed
    JSON_VALUE(event_extra.provider_name) AS provider_name,
    JSON_VALUE(event_extra.provider_id) AS provider_id,
    JSON_VALUE(event_extra.partner_code) AS partner_code,
    JSON_VALUE(event_extra.overridden_by_third_party) AS overridden_by_third_party
  FROM
    `mozdata.firefox_desktop.events_stream`
  WHERE
    -- date(submission_timestamp) = @submission_date
    -- this is for test runs
    DATE(submission_timestamp) = '2025-06-07'
    -- and sample_id = 0
    AND event = 'sap.counts'
),
sap_clients_events_adblocker_is_enterprise_cte AS (
  SELECT
    sap_clients_events_cte.subsession_start_date,
    sap_clients_events_cte.submission_date,
    sap_clients_events_cte.client_id,
    sap_clients_events_cte.normalized_engine,
    sap_clients_events_cte.source,
    -- sap_clients_events_cte.source as search_access_point, -- rename
    sap_clients_events_cte.country,
    sap_clients_events_cte.app_version,
    sap_clients_events_cte.channel,
    sap_clients_events_cte.normalized_channel,
    sap_clients_events_cte.locale,
    sap_clients_events_cte.os,
    sap_clients_events_cte.normalized_os,
    sap_clients_events_cte.os_version,
    sap_clients_events_cte.normalized_os_version,
    sap_clients_events_cte.profile_creation_date,
    sap_clients_events_cte.sample_id,
    sap_clients_events_cte.profile_group_id,
    sap_clients_events_cte.legacy_telemetry_client_id,
    sap_clients_events_cte.experiments,
    sap_clients_events_cte.provider_name,
    sap_clients_events_cte.provider_id,
    sap_clients_events_cte.partner_code,
    sap_clients_events_cte.overridden_by_third_party,
    clients_with_adblocker_addons_cte.has_adblocker_addon,
    is_enterprise_cte.policies_is_enterprise
  FROM
    sap_clients_events_cte
  LEFT JOIN
    clients_with_adblocker_addons_cte
    USING (client_id, submission_date)
  LEFT JOIN
    is_enterprise_cte
    USING (client_id, submission_date)
),
sap_aggregates_cte AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    CASE
      WHEN JSON_VALUE(event_extra.provider_id) = 'other'
        THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
            JSON_VALUE(event_extra.provider_name)
          )
      ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_id)
        )
    END AS normalized_engine,
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
    COUNTIF(ping_info.seq = 1) AS sessions_started_on_this_day
  FROM
    `mozdata.firefox_desktop.events_stream`
  WHERE
  -- date(submission_timestamp) = @submission_date
  -- this is for test runs
    DATE(submission_timestamp) = '2025-06-07'
  -- and sample_id = 0
    AND event = 'sap.counts'
  GROUP BY
    submission_date,
    client_id,
    normalized_engine,
    source
  -- search_access_point
),
final_sap_cte AS (
  SELECT
    sap_clients_events_adblocker_is_enterprise_cte.subsession_start_date,
    sap_clients_events_adblocker_is_enterprise_cte.submission_date,
    sap_clients_events_adblocker_is_enterprise_cte.client_id,
    sap_clients_events_adblocker_is_enterprise_cte.normalized_engine,
    sap_clients_events_adblocker_is_enterprise_cte.source,
  -- sap_clients_events_adblocker_is_enterprise_cte.search_access_point, -- rename
    sap_clients_events_adblocker_is_enterprise_cte.country,
    sap_clients_events_adblocker_is_enterprise_cte.app_version,
    sap_clients_events_adblocker_is_enterprise_cte.locale,
    sap_clients_events_adblocker_is_enterprise_cte.os,
    sap_clients_events_adblocker_is_enterprise_cte.os_version,
    sap_clients_events_adblocker_is_enterprise_cte.profile_creation_date,
    sap_clients_events_adblocker_is_enterprise_cte.channel,
    sap_clients_events_adblocker_is_enterprise_cte.normalized_channel,
    sap_clients_events_adblocker_is_enterprise_cte.normalized_os,
    sap_clients_events_adblocker_is_enterprise_cte.normalized_os_version,
    sap_clients_events_adblocker_is_enterprise_cte.sample_id,
    sap_clients_events_adblocker_is_enterprise_cte.profile_group_id,
    sap_clients_events_adblocker_is_enterprise_cte.legacy_telemetry_client_id,
    sap_clients_events_adblocker_is_enterprise_cte.experiments,
    sap_clients_events_adblocker_is_enterprise_cte.provider_name,
    sap_clients_events_adblocker_is_enterprise_cte.provider_id,
    sap_clients_events_adblocker_is_enterprise_cte.partner_code,
    sap_clients_events_adblocker_is_enterprise_cte.overridden_by_third_party,
    sap_clients_events_adblocker_is_enterprise_cte.has_adblocker_addon,
    sap_clients_events_adblocker_is_enterprise_cte.policies_is_enterprise,
    sap_aggregates_cte.profile_age_in_days,
    sap_aggregates_cte.sap,
    sap_aggregates_cte.sessions_started_on_this_day
  FROM
    sap_clients_events_adblocker_is_enterprise_cte
  LEFT JOIN
    sap_aggregates_cte
    USING (client_id, submission_date, normalized_engine, source)
    -- using(client_id, submission_date, normalized_engine, search_access_point) -- rename
),
serp_array_aggs_cte AS (
  SELECT
    submission_date,
    glean_client_id AS client_id,
    sap_source AS serp_search_access_point,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(search_engine) AS serp_provider_id,
    ARRAY_AGG(partner_code IGNORE NULLS) AS serp_partner_code,
    ARRAY_AGG(ad_blocker_inferred) AS serp_ad_blocker_inferred
  FROM
    `mozdata.firefox_desktop.serp_events`
  WHERE
    -- date(submission_timestamp) = @submission_date
    -- this is for test runs
    submission_date = '2025-06-07'
    -- and sample_id = 0
  GROUP BY
    submission_date,
    client_id,
    serp_provider_id,
    serp_search_access_point
),
serp_aggregates_cte AS (
  SELECT
    submission_date,
    glean_client_id AS client_id,
    sap_source AS serp_search_access_point,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(search_engine) AS serp_provider_id,
    COUNTIF(
      (is_tagged IS TRUE)
      AND (
        sap_source = 'follow_on_from_refine_on_incontent_search'
        OR sap_source = 'follow_on_from_refine_on_SERP'
      )
    ) AS serp_follow_on_searches_tagged_count,
    COUNTIF(is_tagged IS TRUE) AS serp_searches_tagged_count,
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
    SUM(num_ads_notshowing) AS num_ads_notshowing
  FROM
    `mozdata.firefox_desktop.serp_events`
  WHERE
  -- date(submission_timestamp) = @submission_date
  -- this is for test runs
    submission_date = '2025-06-07'
  -- and sample_id = 0
  GROUP BY
    submission_date,
    client_id,
    serp_provider_id,
    serp_search_access_point
),
serp_ad_click_target_cte AS (
  SELECT
    submission_date,
    glean_client_id AS client_id,
    sap_source AS serp_search_access_point,
    `moz-fx-data-shared-prod.udf.normalize_search_engine`(search_engine) AS serp_provider_id,
    ARRAY_AGG(DISTINCT ad_components.component) AS ad_click_target
  FROM
    `mozdata.firefox_desktop.serp_events`
  CROSS JOIN
    UNNEST(ad_components) AS ad_components
  WHERE
    submission_date = '2025-06-07'
  -- and sample_id = 0
  GROUP BY
    submission_date,
    client_id,
    serp_provider_id,
    serp_search_access_point
),
final_serp_cte AS (
  SELECT
    serp_array_aggs_cte.submission_date,
    serp_array_aggs_cte.client_id,
    serp_array_aggs_cte.serp_provider_id,
    serp_array_aggs_cte.serp_search_access_point,
    serp_array_aggs_cte.serp_partner_code,
    serp_array_aggs_cte.serp_ad_blocker_inferred,
    serp_aggregates_cte.serp_searches_tagged_count,
    serp_aggregates_cte.serp_searches_organic_count,
    serp_aggregates_cte.serp_with_ads_organic_count,
    serp_aggregates_cte.serp_ad_clicks_tagged_count,
    serp_aggregates_cte.serp_ad_clicks_organic_count,
    serp_aggregates_cte.num_ad_clicks,
    serp_aggregates_cte.num_non_ad_link_clicks,
    serp_aggregates_cte.num_other_engagements,
    serp_aggregates_cte.num_ads_loaded,
    serp_aggregates_cte.num_ads_visible,
    serp_aggregates_cte.num_ads_blocked,
    serp_aggregates_cte.num_ads_notshowing,
    serp_ad_click_target_cte.ad_click_target
  FROM
    serp_array_aggs_cte
  LEFT JOIN
    serp_aggregates_cte
    USING (client_id, submission_date, serp_provider_id, serp_search_access_point)
  LEFT JOIN
    serp_ad_click_target_cte
    USING (client_id, submission_date, serp_provider_id, serp_search_access_point)
),
glean_metrics_cte AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    sap.normalized_engine,
    sap.source,
  -- sap.search_access_point, -- rename
    client_info.distribution.name AS distribution_id,
    client_info.windows_build_number AS windows_build_number,
    metrics.string.region_home_region AS user_pref_browser_search_region,
    metrics.labeled_counter.browser_is_user_default AS is_default_browser,
  -- metrics.labeled_counter.browser_is_user_default as browser_is_user_default -- rename
    metrics.string.search_engine_default_display_name AS default_search_engine,
  -- metrics.string.search_engine_default_display_name as default_search_engine_display_name, -- rename
    metrics.string.search_engine_default_load_path AS default_search_engine_data_load_path,
  -- metrics.string.search_engine_default_load_path as default_search_engine_load_path, -- rename
    metrics.url2.search_engine_default_submission_url AS default_search_engine_data_submission_url,
  -- metrics.url2.search_engine_default_submission_url as default_search_engine_submission_url, -- rename
    metrics.string.search_engine_private_display_name AS default_private_search_engine,
  -- metrics.string.search_engine_private_display_name as default_private_search_engine_display_name, -- rename
    metrics.string.search_engine_private_load_path AS default_private_search_engine_data_load_path,
  -- metrics.string.search_engine_private_load_path as default_private_search_engine_load_path, -- rename
    metrics.url2.search_engine_private_submission_url AS default_private_search_engine_data_submission_url,
  -- metrics.url2.search_engine_private_submission_url as default_private_search_engine_submission_url, -- rename
    metrics.string.search_engine_default_provider_id AS default_search_engine_provider_id,
    metrics.string.search_engine_default_partner_code AS default_search_engine_partner_code,
    metrics.boolean.search_engine_default_overridden_by_third_party AS default_search_engine_overridden
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1` AS metrics_v1
  LEFT JOIN
    sap_clients_events_cte AS sap
    ON metrics_v1.client_info.client_id = sap.client_id
    AND DATE(metrics_v1.submission_timestamp) = sap.submission_date
  WHERE
  -- date(submission_timestamp) = @submission_date
  -- this is for test runs
    DATE(metrics_v1.submission_timestamp) = '2025-06-07'
  -- and metrics_v1.sample_id = 0
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_info.client_id
      ORDER BY
        submission_timestamp DESC
    ) = 1 -- this is to get the last instance
),
glean_aggregates_cte AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    sap.normalized_engine,
    sap.source,
  -- sap.search_access_point, -- rename
    SUM(metrics.counter.browser_engagement_active_ticks / (3600 / 5)) AS active_hours_sum,
    SUM(
      metrics.counter.browser_engagement_tab_open_event_count
    ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(
      metrics.counter.browser_engagement_uri_count
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    MAX(
      metrics.quantity.browser_engagement_max_concurrent_tab_count
    ) AS max_concurrent_tab_count_max
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1` AS metrics_v1
  LEFT JOIN
    sap_clients_events_cte AS sap
    ON metrics_v1.client_info.client_id = sap.client_id
    AND DATE(metrics_v1.submission_timestamp) = sap.submission_date
  WHERE
  -- date(submission_timestamp) = @submission_date
  -- this is for test runs
    DATE(submission_timestamp) = '2025-06-07'
  -- and metrics_v1.sample_id = 0
  GROUP BY
    client_id,
    submission_date,
    sap.normalized_engine,
    sap.source
  -- sap.search_access_point -- rename
),
final_glean_cte AS (
  SELECT
    glean_metrics_cte.client_id,
    glean_metrics_cte.submission_date,
    glean_metrics_cte.normalized_engine,
    glean_metrics_cte.source,
  -- glean_metrics_cte.search_access_point, -- rename
    glean_metrics_cte.windows_build_number,
    glean_metrics_cte.distribution_id,
    glean_metrics_cte.user_pref_browser_search_region,
    glean_metrics_cte.is_default_browser,
  -- glean_metrics_cte.browser_is_user_default, -- rename
    glean_metrics_cte.default_search_engine,
  -- glean_metrics_cte.default_search_engine_display_name, -- rename
    glean_metrics_cte.default_search_engine_data_load_path,
  -- glean_metrics_cte.default_search_engine_load_path, -- rename
    glean_metrics_cte.default_search_engine_data_submission_url,
  -- glean_metrics_cte.default_search_engine_submission_url, -- rename
    glean_metrics_cte.default_private_search_engine,
  -- glean_metrics_cte.default_private_search_engine_display_name, -- rename
    glean_metrics_cte.default_private_search_engine_data_load_path,
  -- glean_metrics_cte.default_private_search_engine_load_path, -- rename
    glean_metrics_cte.default_private_search_engine_data_submission_url,
  -- glean_metrics_cte.default_private_search_engine_submission_url, -- rename
    glean_metrics_cte.default_search_engine_provider_id,
    glean_metrics_cte.default_search_engine_partner_code,
    glean_metrics_cte.default_search_engine_overridden,
    glean_aggregates_cte.active_hours_sum,
    glean_aggregates_cte.scalar_parent_browser_engagement_tab_open_event_count_sum,
    glean_aggregates_cte.scalar_parent_browser_engagement_total_uri_count_sum,
    glean_aggregates_cte.max_concurrent_tab_count_max
  FROM
    glean_metrics_cte
  LEFT JOIN
    glean_aggregates_cte
    USING (client_id, submission_date, normalized_engine, source)
    -- using(client_id, submission_date, normalized_engine, search_access_point) -- rename
),
final_joined_cte AS (
  SELECT
    final_sap_cte.sample_id,
    final_sap_cte.submission_date,
    final_sap_cte.client_id,
    final_sap_cte.legacy_telemetry_client_id,
    final_sap_cte.normalized_engine,
    final_sap_cte.source,
  -- final_sap_cte.search_access_point, -- rename
    final_serp_cte.serp_search_access_point,
    final_sap_cte.provider_name,
    final_sap_cte.provider_id,
    final_serp_cte.serp_provider_id,
    final_sap_cte.partner_code,
    final_serp_cte.serp_partner_code,
    final_sap_cte.country,
    final_sap_cte.app_version,
    final_glean_cte.windows_build_number,
    final_glean_cte.distribution_id,
    final_sap_cte.locale,
    final_glean_cte.user_pref_browser_search_region,
    final_sap_cte.os,
    final_sap_cte.normalized_os,
    final_sap_cte.os_version,
    final_sap_cte.normalized_os_version,
    final_sap_cte.channel,
    final_sap_cte.normalized_channel,
    final_glean_cte.is_default_browser,
  -- final_glean_cte.browser_is_user_default, -- rename
    final_sap_cte.profile_creation_date,
    final_sap_cte.profile_age_in_days,
    final_sap_cte.profile_group_id,
    final_glean_cte.default_search_engine,
  -- final_glean_cte.default_search_engine_display_name, -- rename
    final_glean_cte.default_search_engine_data_load_path,
  -- final_glean_cte.default_search_engine_load_path, -- rename
    final_glean_cte.default_search_engine_data_submission_url,
  -- final_glean_cte.default_search_engine_submission_url, -- rename
    final_glean_cte.default_private_search_engine,
  -- final_glean_cte.default_private_search_engine_display_name, -- rename
    final_glean_cte.default_private_search_engine_data_load_path,
  -- final_glean_cte.default_private_search_engine_load_path, -- rename
    final_glean_cte.default_private_search_engine_data_submission_url,
  -- final_glean_cte.default_private_search_engine_submission_url, -- rename
    final_sap_cte.sessions_started_on_this_day,
    final_glean_cte.max_concurrent_tab_count_max,
    final_glean_cte.scalar_parent_browser_engagement_tab_open_event_count_sum,
    final_glean_cte.active_hours_sum,
    final_glean_cte.scalar_parent_browser_engagement_total_uri_count_sum,
    final_sap_cte.experiments,
    final_sap_cte.policies_is_enterprise,
    final_glean_cte.default_search_engine_provider_id,
    final_glean_cte.default_search_engine_partner_code,
    final_glean_cte.default_search_engine_overridden,
    final_sap_cte.overridden_by_third_party,
    final_sap_cte.has_adblocker_addon,
    final_sap_cte.sap,
    final_serp_cte.serp_ad_blocker_inferred,
    final_serp_cte.serp_searches_tagged_count,
    final_serp_cte.serp_searches_organic_count,
    final_serp_cte.serp_with_ads_organic_count,
    final_serp_cte.serp_ad_clicks_tagged_count,
    final_serp_cte.serp_ad_clicks_organic_count,
    final_serp_cte.num_ad_clicks,
    final_serp_cte.num_non_ad_link_clicks,
    final_serp_cte.num_other_engagements,
    final_serp_cte.num_ads_loaded,
    final_serp_cte.num_ads_visible,
    final_serp_cte.num_ads_blocked,
    final_serp_cte.num_ads_notshowing,
    final_serp_cte.ad_click_target
  FROM
    final_sap_cte
  LEFT JOIN
    final_serp_cte
    ON final_sap_cte.client_id = final_serp_cte.client_id
    AND final_sap_cte.submission_date = final_serp_cte.submission_date
    AND final_sap_cte.normalized_engine = final_serp_cte.serp_provider_id
    AND final_sap_cte.source = final_serp_cte.serp_search_access_point
    -- and final_sap_cte.search_access_point = final_serp_cte.serp_search_access_point -- rename
  LEFT JOIN
    final_glean_cte
    ON final_sap_cte.client_id = final_glean_cte.client_id
    AND final_sap_cte.submission_date = final_glean_cte.submission_date
    AND final_sap_cte.normalized_engine = final_glean_cte.normalized_engine
    AND final_sap_cte.source = final_glean_cte.source
    -- and final_sap_cte.search_access_point = final_glean_cte.search_access_point -- rename
)
SELECT
  *
FROM
  final_joined_cte
