-- Parse timestamp (with support for different timestamp formats)
CREATE TEMP FUNCTION SAFE_PARSE_TIMESTAMP(ts STRING) AS (
    -- Ref for time format elements: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements
  CASE
    -- e.g. "2025-05-01T15:45+03:30"
    WHEN REGEXP_CONTAINS(ts, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(\-|\+)\d{2}:\d{2}")
      THEN SAFE.PARSE_TIMESTAMP("%FT%R%Ez", ts)
    -- e.g. "2025-05-01+03:30"
    WHEN REGEXP_CONTAINS(ts, r"\d{4}-\d{2}-\d{2}(\-|\+)\d{2}:\d{2}")
      THEN SAFE.PARSE_TIMESTAMP("%F%Ez", ts)
    -- e.g. "2025-05-01T14:44:20.365-04:00"
    WHEN REGEXP_CONTAINS(ts, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d*(\-|\+)\d{2}:\d{2}")
      THEN SAFE.PARSE_TIMESTAMP("%FT%R:%E*S%Ez", ts)
    ELSE NULL
  END
);

-- List of Ad Blocking Addons produced using this logic: https://github.com/mozilla/search-adhoc-analysis/tree/master/monetization-blocking-addons
WITH
adblocker_addons AS (
  SELECT
  addon_id,
  addon_name
  FROM
  moz-fx-data-shared-prod.revenue.monetization_blocking_addons
  WHERE
  blocks_monetization
),
-- This table is the Glean equivalent to moz-fx-data-shared-prod.telemetry.clients_daily
clients_with_adblocker_addons AS (
  SELECT
  client_info.client_id,
  date(submission_timestamp),
  TRUE AS has_adblocker_addon
  FROM
  moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1,
  UNNEST(JSON_QUERY_ARRAY(metrics.object.addons_active_addons)) AS addons
  INNER JOIN
  adblocker_addons
    on adblocker_addons.addon_id = JSON_VALUE(addons, '$.id')
    WHERE
    -- this is for test runs
    -- date(submission_timestamp) = '2025-06-01'
    -- and sample_id = 0
    date(submission_timestamp) = @submission_date
    and not BOOL(JSON_QUERY(addons, '$.userDisabled'))
    and not BOOL(JSON_QUERY(addons, '$.appDisabled'))
    and not BOOL(JSON_QUERY(addons, '$.blocklisted'))
    GROUP BY
    client_id,
    date(submission_timestamp)
),

-- We still need this because of document_id IS NOT NULL
is_enterprise_policies AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    mozfun.stats.mode_last(
      ARRAY_AGG(metrics.boolean.policies_is_enterprise ORDER BY submission_timestamp)
    ) AS policies_is_enterprise
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    -- this is for test runs
    -- date(submission_timestamp) = '2025-06-01'
    -- and sample_id = 0
    date(submission_timestamp) = @submission_date
    AND document_id IS NOT NULL
  GROUP BY
    client_id,
    submission_date
),

-- join serp to sap including as much client info as possible via sap.counts ping
-- this cte gets client_info and sap counts from the sap.counts events ping and aggregates at client_id, day
sap_clients_events AS (
  SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS client_id,
  client_info.app_channel AS channel,
  normalized_app_name AS app_version,
  client_info.locale AS locale,
  client_info.os AS os,
  client_info.os_version AS os_version,
  UNIX_DATE(DATE(SAFE_PARSE_TIMESTAMP(client_info.first_run_date))) AS profile_creation_date,
  client_info.session_count AS session_count,
  client_info.windows_build_number AS windows_build_number,
  client_info.distribution.name AS distribution_id,
  normalized_channel,
  normalized_country_code as country,
  normalized_os,
  normalized_os_version,
  sample_id,
  metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
  metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id, -- adding this for now so people can join to it if needed
  ping_info.experiments as ping_info_experiments,
  ping_info.parsed_start_time as subsession_start_date,
  ping_info.end_time as subsession_end_time,
  ping_info.seq as subsession_counter,
  -- SYDNEY AND MARK: we need alignment on whether to keep the normalized_engine name or provider_name
  mozfun.map.get_key(event.extra, 'provider_name') as normalized_engine,
  mozfun.map.get_key(event.extra, 'provider_name') as provider_name,
  mozfun.map.get_key(event.extra, 'provider_id') as provider_id,
  mozfun.map.get_key(event.extra, 'partner_code') as partner_code,
  mozfun.map.get_key(event.extra, 'overridden_by_third_party') as overridden_by_third_party,
  mozfun.map.get_key(event.extra, 'source') as search_access_point,
 FROM firefox_desktop.events as e
 CROSS JOIN UNNEST(e.events) as event
  WHERE
    -- this is for test runs
    -- date(submission_timestamp) = '2025-06-01'
    -- and sample_id = 0
    date(submission_timestamp) = @submission_date
    AND event.category = 'sap'
    AND event.name = 'counts'
),

-- this CTE gets serp event data
serp_events AS (
  SELECT
    submission_date,
    impression_id,
    glean_client_id as client_id,
    -- SYDNEY AND MARK: do we even need this serp_search_access_point? If we have it from the sap_count ping?
    sap_source as serp_search_access_point,
    is_tagged,
    num_ad_clicks,
    num_non_ad_link_clicks,
    e.num_other_engagements,
    num_ads_loaded,
    num_ads_visible,
    num_ads_blocked,
    num_ads_notshowing,
    ad_blocker_inferred,
    ad_components.component as ad_click_target
  FROM `mozdata.firefox_desktop.serp_events` e
  CROSS JOIN UNNEST(ad_components) as ad_components
  WHERE
    submission_date = @submission_date
    -- this is for test runs
    -- submission_date = '2025-06-01'
    -- and sample_id = 0
),

-- this CTE gets all client info and other metrics that are not available in the glean sap.counts events ping
glean_metrics AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.search_engine_default_display_name AS default_search_engine,
    metrics.string.search_engine_default_load_path AS default_search_engine_data_load_path,
    metrics.url2.search_engine_default_submission_url AS default_search_engine_data_submission_url,
    metrics.string.search_engine_private_display_name AS default_private_search_engine,
    metrics.string.search_engine_private_load_path AS default_private_search_engine_data_load_path,
    metrics.url2.search_engine_private_submission_url AS default_private_search_engine_data_submission_url,
    metrics.string.region_home_region AS user_pref_browser_search_region,
    metrics.labeled_counter.browser_is_user_default as is_default_browser,
    metrics.quantity.browser_engagement_max_concurrent_tab_count as browser_engagement_max_concurrent_tab_count,
    metrics.counter.browser_engagement_active_ticks as browser_engagement_active_ticks,
    metrics.counter.browser_engagement_tab_open_event_count as browser_engagement_tab_open_event_count,
    metrics.counter.browser_engagement_uri_count
    -- SYDNEY AND MARK: do you want that main-ping-like session counts but using glean event timing and sequences?
    -- ping_info.start_time AS subsession_start_date, -- ping_info.parsed_start_time instead?
    -- ping_info.seq AS subsession_counter,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    date(submission_timestamp) = @submission_date
    -- this is for test runs
    -- date(submission_timestamp) = '2025-06-01'
    -- and sample_id = 0
),

-- This CTE joins sap_events_clients, serp_events, and glean_metrics

sap_serp_glean AS (
  SELECT * FROM sap_clients_events
  LEFT JOIN serp_events USING(client_id, submission_date)
  LEFT JOIN glean_metrics USING(client_id, submission_date)
),

-- This CTE aggregates any fields that need to be counted or summed

sap_serp_glean_aggregates AS (

   SELECT
      client_id,
      submission_date,
      -- this is sap_clients_events stuff getting aggregated
      COUNT(*) AS sap,
      -- SYDNEY AND MARK: do we need subsession_hours_sum? This existed in the main ping. We can extract something similar from Glean, but do we need it?
      -- SUM(
  --     TIMESTAMP_DIFF(
  --       SAFE_PARSE_TIMESTAMP(subsession_end_time),
  --       SAFE_PARSE_TIMESTAMP(subsession_start_date),
  --       SECOND
  --     ) / NUMERIC '3600'
  -- ) AS subsession_hours_sum,
      -- this is serp_events stuff getting aggregated
      COUNT(DISTINCT impression_id) AS serp_impression_count,
      COUNTIF(is_tagged = TRUE) AS serp_searches_tagged_count,
      COUNTIF(is_tagged = FALSE) AS serp_organic_count,
      -- MARK: is this the right way to get follow on searches?
      COUNTIF((is_tagged = TRUE)
        AND (search_access_point = 'follow_on_from_refine_on_incontent_search'
        OR search_access_point = 'follow_on_from_refine_on_SERP')) AS serp_follow_on_searches_tagged_count,
      COUNTIF(is_tagged = FALSE AND num_ads_visible > 0) AS serp_with_ads_organic_count,
      COUNTIF(is_tagged = TRUE AND num_ads_visible > 0) AS serp_with_ads_tagged_count,
      SUM(IF(is_tagged = FALSE, num_ad_clicks, 0)) AS serp_ad_clicks_organic_count,
      SUM(IF(is_tagged = TRUE, num_ad_clicks, 0)) AS serp_ad_clicks_tagged_count,
      SUM(num_ad_clicks) AS serp_ad_click_sum,
      ARRAY_AGG(ad_blocker_inferred IGNORE NULLS) AS serp_ad_blocker_inferred,
      ARRAY_AGG(ad_click_target IGNORE NULLS) as serp_ad_clicks_target,
      ARRAY_AGG(impression_id IGNORE NULLS) AS serp_impression_id,
      --this is glean_metrics stuff getting aggregated
      SUM(browser_engagement_active_ticks / (3600 / 5)) AS active_hours_sum,
      -- This is commented out because we're not yet sure if we need it (line 190)
      -- SUM(
      --   TIMESTAMP_DIFF(
      --     SAFE_PARSE_TIMESTAMP(subsession_end_time),
      --     SAFE_PARSE_TIMESTAMP(subsession_start_time),
      --     SECOND
      --     ) / NUMERIC '3600'
      --   ) AS subsession_hours_sum,
    COUNTIF(subsession_counter = 1) AS sessions_started_on_this_day,
    SUM(
      browser_engagement_tab_open_event_count
    ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(
      browser_engagement_uri_count
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    MAX(
      browser_engagement_max_concurrent_tab_count
    ) AS max_concurrent_tab_count_max,
    MAX(UNIX_DATE(DATE(SAFE_PARSE_TIMESTAMP(string(subsession_start_date))))) - MAX(
      profile_creation_date
    ) AS profile_age_in_days,
    mozfun.map.mode_last(
      ARRAY_CONCAT_AGG(ping_info_experiments ORDER BY sap_serp_glean.submission_date)
    ) AS experiments
  FROM sap_serp_glean
  GROUP BY
    client_id,
    submission_date
),

--join sap_serp_glean_aggregates to sap_serp_glean
sap_serp_glean_aggregates_joined AS (
  SELECT * FROM sap_serp_glean
  LEFT JOIN sap_serp_glean_aggregates
  USING(client_id, submission_date)
),

-- aggregate by client_id, submission_date, engine, source
clients_engine_sources_daily AS (
  SELECT

  --sap stuff
  client_id,
  submission_date,
  channel,
  app_version,
  locale,
  os,
  os_version,
  profile_creation_date,
  session_count,
  windows_build_number,
  distribution_id,
  normalized_channel,
  country,
  normalized_os,
  normalized_os_version,
  sample_id,
  profile_group_id,
  legacy_telemetry_client_id, -- adding this for now so people can join to it if needed
  experiments,
  normalized_engine,
  provider_name,
  provider_id,
  partner_code,
  overridden_by_third_party,
  search_access_point,

  --sap aggregates
  sap,
  -- SYDNEY AND MARK: do we use sap or sap_count (this is the same as sap)?
  sap AS sap_count,

  --glean metrics
  default_search_engine,
  default_search_engine_data_load_path,
  default_search_engine_data_submission_url,
  default_private_search_engine,
  default_private_search_engine_data_load_path,
  default_private_search_engine_data_submission_url,
  user_pref_browser_search_region,
  is_default_browser,

  --serp events
  ad_blocker_inferred,
  ad_click_target

  --serp aggregates
  serp_impression_count,
  serp_searches_tagged_count,
  serp_organic_count,
  serp_follow_on_searches_tagged_count,
  serp_with_ads_organic_count,
  serp_with_ads_tagged_count,
  serp_ad_clicks_organic_count,
  serp_ad_clicks_tagged_count,
  serp_ad_click_sum,
  serp_ad_blocker_inferred,
  serp_ad_clicks_target,
  serp_impression_id,

  --glean aggregates
  active_hours_sum,
  -- Commenting this out because the reference above is also commented out (lines 190 and 215)
  -- subsession_hours_sum,
  sessions_started_on_this_day,
  scalar_parent_browser_engagement_tab_open_event_count_sum,
  scalar_parent_browser_engagement_total_uri_count_sum,
  max_concurrent_tab_count_max,
  profile_age_in_days

  from sap_serp_glean_aggregates_joined

)

SELECT * FROM clients_engine_sources_daily
