-- Return the version of the search addon if it exists, null otherwise
CREATE TEMP FUNCTION GET_SEARCH_ADDON_VERSION(active_addons ANY type) AS (
  (
    SELECT
      mozfun.stats.mode_last(ARRAY_AGG(version))
    FROM
      UNNEST(active_addons)
    WHERE
      addon_id = 'followonsearch@mozilla.com'
    GROUP BY
      version
  )
);

-- For newer search probes that are based on access point
CREATE TEMP FUNCTION ADD_ACCESS_POINT(
  entries ARRAY<STRUCT<key STRING, value INT64>>,
  access_point STRING
) AS (
  ARRAY(SELECT AS STRUCT CONCAT(key, '.', access_point) AS key, value, FROM UNNEST(entries))
);

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
WITH adblocker_addons AS (
  SELECT
    addon_id,
    addon_name
  FROM
    `moz-fx-data-shared-prod.revenue.monetization_blocking_addons`
  WHERE
    blocks_monetization
),
clients_with_adblocker_addons AS (
  -- no glean equivalent as yet for the active_addons field
  -- is metrics.object.addons_active_addons the correct field?
  -- when it's added we'll update this CTE to pull from the correct glean table (and field)
  SELECT
    client_id,
    submission_date,
    TRUE AS has_adblocker_addon
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  CROSS JOIN
    UNNEST(active_addons) a
  INNER JOIN
    adblocker_addons
    USING (addon_id)
  WHERE
    submission_date = @submission_date
    AND NOT a.user_disabled
    AND NOT a.app_disabled
    AND NOT a.blocklisted
  GROUP BY
    client_id,
    submission_date
),
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
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = 'Firefox'
    AND document_id IS NOT NULL
  GROUP BY
    client_id,
    submission_date
),
-- START: deriving equivalent for clients_daily_v6
-- REF: https://github.com/mozilla/bigquery-etl/blob/generated-sql/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql
non_aggregates AS (
  -- equivalent to clients_summary in clients_daily_v6
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.app_channel AS channel,
    client_info.app_display_version AS app_version,
    client_info.client_id AS client_id,
    client_info.distribution.name AS distribution_id,
    client_info.locale AS locale,
    client_info.os AS os,
    client_info.os_version AS os_version,
    UNIX_DATE(DATE(SAFE_PARSE_TIMESTAMP(client_info.first_run_date))) AS profile_creation_date,
    client_info.windows_build_number AS windows_build_number,
    metadata.geo.country AS country,
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    metrics.string.search_engine_default_display_name AS default_search_engine,
    metrics.string.search_engine_default_load_path AS default_search_engine_data_load_path,
    metrics.url2.search_engine_default_submission_url AS default_search_engine_data_submission_url,
    metrics.string.search_engine_private_display_name AS default_private_search_engine,
    metrics.string.search_engine_private_load_path AS default_private_search_engine_data_load_path,
    metrics.url2.search_engine_private_submission_url AS default_private_search_engine_data_submission_url,
    metrics.string.region_home_region AS user_pref_browser_search_region,
    ping_info.start_time AS subsession_start_date, -- ping_info.parsed_start_time instead?
    ping_info.seq AS subsession_counter,
    sample_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
aggregates AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    SUM(metrics.counter.browser_engagement_active_ticks / (3600 / 5)) AS active_hours_sum,
    SUM(
      TIMESTAMP_DIFF(
        SAFE_PARSE_TIMESTAMP(ping_info.end_time),
        SAFE_PARSE_TIMESTAMP(ping_info.start_time),
        SECOND
      ) / NUMERIC '3600'
    ) AS subsession_hours_sum,
    COUNTIF(ping_info.seq = 1) AS sessions_started_on_this_day,
    SUM(
      metrics.counter.browser_engagement_tab_open_event_count
    ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(
      metrics.counter.browser_engagement_uri_count
    ) AS scalar_parent_browser_engagement_total_uri_count_sum,
    MAX(
      metrics.quantity.browser_engagement_max_concurrent_tab_count
    ) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    MAX(UNIX_DATE(DATE(SAFE_PARSE_TIMESTAMP(ping_info.start_time)))) - MAX(
      UNIX_DATE(DATE(SAFE_PARSE_TIMESTAMP(client_info.first_run_date)))
    ) AS profile_age_in_days,
    mozfun.map.mode_last(
      ARRAY_CONCAT_AGG(ping_info.experiments ORDER BY submission_timestamp)
    ) AS experiments,
    [
      -- metrics.labeled_counter.urlbar_searchmode_*
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_bookmarkmenu) AS agg),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_keywordoffer)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_oneoff)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_other)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_shortcut)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_tabmenu)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_tabtosearch)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_tabtosearch_onboard)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_topsites_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_topsites_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_touchbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.urlbar_searchmode_typed)),
      -- metrics.labeled_counter.browser_search_content_*
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_reload)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_system)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_content_webextension)),
      -- metrics.labeled_counter.browser_search_withads_*
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_reload)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_system)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_withads_webextension)),
      -- metrics.labeled_counter.browser_search_adclicks_*
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_reload)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_system)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_search_adclicks_webextension)),
      STRUCT(ARRAY_CONCAT_AGG(metrics.labeled_counter.browser_is_user_default))
    ] AS map_sum_aggregates,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id,
    submission_date
),
udf_aggregates AS (
  SELECT
    * REPLACE (
      ARRAY(
        SELECT AS STRUCT
          mozfun.map.sum(agg) AS map,
        FROM
          UNNEST(map_sum_aggregates)
      ) AS map_sum_aggregates
    )
  FROM
    aggregates
),
clients_daily_v6 AS (
  SELECT
    * EXCEPT (map_sum_aggregates),
    -- metrics.labeled_counter.urlbar_searchmode_*
    map_sum_aggregates[OFFSET(0)].map AS scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
    map_sum_aggregates[OFFSET(1)].map AS scalar_parent_urlbar_searchmode_handoff_sum,
    map_sum_aggregates[OFFSET(2)].map AS scalar_parent_urlbar_searchmode_keywordoffer_sum,
    map_sum_aggregates[OFFSET(3)].map AS scalar_parent_urlbar_searchmode_oneoff_sum,
    map_sum_aggregates[OFFSET(4)].map AS scalar_parent_urlbar_searchmode_other_sum,
    map_sum_aggregates[OFFSET(5)].map AS scalar_parent_urlbar_searchmode_shortcut_sum,
    map_sum_aggregates[OFFSET(6)].map AS scalar_parent_urlbar_searchmode_tabmenu_sum,
    map_sum_aggregates[OFFSET(7)].map AS scalar_parent_urlbar_searchmode_tabtosearch_sum,
    map_sum_aggregates[OFFSET(8)].map AS scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
    map_sum_aggregates[OFFSET(9)].map AS scalar_parent_urlbar_searchmode_topsites_newtab_sum,
    map_sum_aggregates[OFFSET(10)].map AS scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
    map_sum_aggregates[OFFSET(11)].map AS scalar_parent_urlbar_searchmode_touchbar_sum,
    map_sum_aggregates[OFFSET(12)].map AS scalar_parent_urlbar_searchmode_typed_sum,
    -- metrics.labeled_counter.browser_search_content_*
    map_sum_aggregates[OFFSET(13)].map AS search_content_about_home_sum,
    map_sum_aggregates[OFFSET(14)].map AS search_content_about_newtab_sum,
    map_sum_aggregates[OFFSET(15)].map AS search_content_contextmenu_sum,
    map_sum_aggregates[OFFSET(16)].map AS search_content_reload_sum,
    map_sum_aggregates[OFFSET(17)].map AS search_content_searchbar_sum,
    map_sum_aggregates[OFFSET(18)].map AS search_content_system_sum,
    map_sum_aggregates[OFFSET(19)].map AS search_content_tabhistory_sum,
    map_sum_aggregates[OFFSET(20)].map AS search_content_unknown_sum,
    map_sum_aggregates[OFFSET(21)].map AS search_content_urlbar_sum,
    map_sum_aggregates[OFFSET(22)].map AS search_content_urlbar_handoff_sum,
    map_sum_aggregates[OFFSET(23)].map AS search_content_urlbar_persisted_sum,
    map_sum_aggregates[OFFSET(24)].map AS search_content_urlbar_searchmode_sum,
    map_sum_aggregates[OFFSET(25)].map AS search_content_webextension_sum,
    -- metrics.labeled_counter.browser_search_withads_*
    map_sum_aggregates[OFFSET(26)].map AS search_withads_about_home_sum,
    map_sum_aggregates[OFFSET(27)].map AS search_withads_about_newtab_sum,
    map_sum_aggregates[OFFSET(28)].map AS search_withads_contextmenu_sum,
    map_sum_aggregates[OFFSET(29)].map AS search_withads_reload_sum,
    map_sum_aggregates[OFFSET(30)].map AS search_withads_searchbar_sum,
    map_sum_aggregates[OFFSET(31)].map AS search_withads_system_sum,
    map_sum_aggregates[OFFSET(32)].map AS search_withads_tabhistory_sum,
    map_sum_aggregates[OFFSET(33)].map AS search_withads_unknown_sum,
    map_sum_aggregates[OFFSET(34)].map AS search_withads_urlbar_sum,
    map_sum_aggregates[OFFSET(35)].map AS search_withads_urlbar_handoff_sum,
    map_sum_aggregates[OFFSET(36)].map AS search_withads_urlbar_persisted_sum,
    map_sum_aggregates[OFFSET(37)].map AS search_withads_urlbar_searchmode_sum,
    map_sum_aggregates[OFFSET(38)].map AS search_withads_webextension_sum,
    -- metrics.labeled_counter.browser_search_adclicks_*
    map_sum_aggregates[OFFSET(39)].map AS search_adclicks_about_home_sum,
    map_sum_aggregates[OFFSET(40)].map AS search_adclicks_about_newtab_sum,
    map_sum_aggregates[OFFSET(41)].map AS search_adclicks_contextmenu_sum,
    map_sum_aggregates[OFFSET(42)].map AS search_adclicks_reload_sum,
    map_sum_aggregates[OFFSET(43)].map AS search_adclicks_searchbar_sum,
    map_sum_aggregates[OFFSET(44)].map AS search_adclicks_system_sum,
    map_sum_aggregates[OFFSET(45)].map AS search_adclicks_tabhistory_sum,
    map_sum_aggregates[OFFSET(46)].map AS search_adclicks_unknown_sum,
    map_sum_aggregates[OFFSET(47)].map AS search_adclicks_urlbar_sum,
    map_sum_aggregates[OFFSET(48)].map AS search_adclicks_urlbar_handoff_sum,
    map_sum_aggregates[OFFSET(49)].map AS search_adclicks_urlbar_persisted_sum,
    map_sum_aggregates[OFFSET(50)].map AS search_adclicks_urlbar_searchmode_sum,
    map_sum_aggregates[OFFSET(51)].map AS search_adclicks_webextension_sum,
    map_sum_aggregates[OFFSET(52)].map AS is_default_browser,
  FROM
    udf_aggregates
)
-- END: deriving equivalent from clients_daily_v6
,
combined_access_point AS (
  SELECT
    * EXCEPT (has_adblocker_addon),
    COALESCE(has_adblocker_addon, FALSE) AS has_adblocker_addon,
    ARRAY_CONCAT(
      ADD_ACCESS_POINT(search_content_about_home_sum, 'about_home'),
      ADD_ACCESS_POINT(search_content_about_newtab_sum, 'about_newtab'),
      ADD_ACCESS_POINT(search_content_contextmenu_sum, 'contextmenu'),
      ADD_ACCESS_POINT(search_content_reload_sum, 'reload'),
      ADD_ACCESS_POINT(search_content_searchbar_sum, 'searchbar'),
      ADD_ACCESS_POINT(search_content_system_sum, 'system'),
      ADD_ACCESS_POINT(search_content_tabhistory_sum, 'tabhistory'),
      ADD_ACCESS_POINT(search_content_unknown_sum, 'unknown'),
      ADD_ACCESS_POINT(search_content_urlbar_sum, 'urlbar'),
      ADD_ACCESS_POINT(search_content_urlbar_handoff_sum, 'urlbar_handoff'),
      ADD_ACCESS_POINT(search_content_urlbar_persisted_sum, 'urlbar_persisted'),
      ADD_ACCESS_POINT(search_content_urlbar_searchmode_sum, 'urlbar_searchmode'),
      ADD_ACCESS_POINT(search_content_webextension_sum, 'webextension')
    ) AS in_content_with_sap,
    ARRAY_CONCAT(
      ADD_ACCESS_POINT(search_withads_about_home_sum, 'about_home'),
      ADD_ACCESS_POINT(search_withads_about_newtab_sum, 'about_newtab'),
      ADD_ACCESS_POINT(search_withads_contextmenu_sum, 'contextmenu'),
      ADD_ACCESS_POINT(search_withads_reload_sum, 'reload'),
      ADD_ACCESS_POINT(search_withads_searchbar_sum, 'searchbar'),
      ADD_ACCESS_POINT(search_withads_system_sum, 'system'),
      ADD_ACCESS_POINT(search_withads_tabhistory_sum, 'tabhistory'),
      ADD_ACCESS_POINT(search_withads_unknown_sum, 'unknown'),
      ADD_ACCESS_POINT(search_withads_urlbar_sum, 'urlbar'),
      ADD_ACCESS_POINT(search_withads_urlbar_handoff_sum, 'urlbar_handoff'),
      ADD_ACCESS_POINT(search_withads_urlbar_persisted_sum, 'urlbar_persisted'),
      ADD_ACCESS_POINT(search_withads_urlbar_searchmode_sum, 'urlbar_searchmode'),
      ADD_ACCESS_POINT(search_withads_webextension_sum, 'webextension')
    ) AS search_with_ads_with_sap,
    ARRAY_CONCAT(
      ADD_ACCESS_POINT(search_adclicks_about_home_sum, 'about_home'),
      ADD_ACCESS_POINT(search_adclicks_about_newtab_sum, 'about_newtab'),
      ADD_ACCESS_POINT(search_adclicks_contextmenu_sum, 'contextmenu'),
      ADD_ACCESS_POINT(search_adclicks_reload_sum, 'reload'),
      ADD_ACCESS_POINT(search_adclicks_searchbar_sum, 'searchbar'),
      ADD_ACCESS_POINT(search_adclicks_system_sum, 'system'),
      ADD_ACCESS_POINT(search_adclicks_tabhistory_sum, 'tabhistory'),
      ADD_ACCESS_POINT(search_adclicks_unknown_sum, 'unknown'),
      ADD_ACCESS_POINT(search_adclicks_urlbar_sum, 'urlbar'),
      ADD_ACCESS_POINT(search_adclicks_urlbar_handoff_sum, 'urlbar_handoff'),
      ADD_ACCESS_POINT(search_adclicks_urlbar_persisted_sum, 'urlbar_persisted'),
      ADD_ACCESS_POINT(search_adclicks_urlbar_searchmode_sum, 'urlbar_searchmode'),
      ADD_ACCESS_POINT(search_adclicks_webextension_sum, 'webextension')
    ) AS ad_clicks_with_sap,
  FROM
    clients_daily_v6
  LEFT JOIN
    clients_with_adblocker_addons
    USING (client_id, submission_date)
  LEFT JOIN
    is_enterprise_policies
    USING (client_id, submission_date)
  LEFT JOIN
    non_aggregates
    USING (client_id, submission_date)
),
augmented AS (
  SELECT
    *,
    ARRAY_CONCAT(
      ARRAY(
        SELECT AS STRUCT
          element.source AS source,
          element.engine AS engine,
          element.count AS count,
          CASE
            WHEN (
                element.source IN (
                  'searchbar',
                  'urlbar',
                  'abouthome',
                  'newtab',
                  'contextmenu',
                  'system',
                  'activitystream',
                  'webextension',
                  'alias',
                  'urlbar-searchmode',
                  'urlbar-handoff',
                  'urlbar-persisted'
                )
                OR element.source IS NULL
              )
              THEN 'sap'
            WHEN STARTS_WITH(element.source, 'in-content:sap:')
              OR STARTS_WITH(element.source, 'sap:')
              THEN 'tagged-sap'
            WHEN STARTS_WITH(element.source, 'in-content:sap-follow-on:')
              OR STARTS_WITH(element.source, 'follow-on:')
              THEN 'tagged-follow-on'
            WHEN STARTS_WITH(element.source, 'in-content:organic:')
              THEN 'organic'
            WHEN STARTS_WITH(element.source, 'ad-click:')
              THEN 'ad-click'
            WHEN STARTS_WITH(element.source, 'search-with-ads:')
              THEN 'search-with-ads'
            ELSE 'unknown'
          END AS type
        FROM
          UNNEST(search_counts) AS element
        WHERE
          -- only use these in-content counts if access point probes are not available
          ARRAY_LENGTH(in_content_with_sap) = 0
          OR NOT STARTS_WITH(element.source, 'in-content:')
      ),
      ARRAY(
        SELECT AS STRUCT
          CONCAT('in-content:', SUBSTR(key, STRPOS(key, ':') + 1)) AS source,
          SPLIT(key, ':')[OFFSET(0)] AS engine,
          value AS count,
          CASE
            WHEN REGEXP_CONTAINS(key, ':tagged:')
              THEN 'tagged-sap'
            WHEN REGEXP_CONTAINS(key, ':tagged-follow-on:')
              THEN 'tagged-follow-on'
            WHEN REGEXP_CONTAINS(key, ':organic:')
              THEN 'organic'
            ELSE 'unknown'
          END AS type
        FROM
          UNNEST(in_content_with_sap)
      ),
      ARRAY(
        SELECT AS STRUCT
          CONCAT('ad-click:', COALESCE(SPLIT(key, ':')[SAFE_OFFSET(1)], '')) AS source,
          SPLIT(key, ':')[OFFSET(0)] AS engine,
          value AS count,
          CONCAT('ad-click', IF(REGEXP_CONTAINS(key, ':organic'), ':organic', '')) AS type
        FROM
          UNNEST(ad_clicks_with_sap)
      ),
      ARRAY(
        SELECT AS STRUCT
          CONCAT('search-with-ads:', COALESCE(SPLIT(key, ':')[SAFE_OFFSET(1)], '')) AS source,
          SPLIT(key, ':')[OFFSET(0)] AS engine,
          value AS count,
          CONCAT('search-with-ads', IF(REGEXP_CONTAINS(key, ':organic'), ':organic', '')) AS type
        FROM
          UNNEST(search_with_ads_with_sap)
      )
    ) AS _searches,
  FROM
    combined_access_point
),
flattened AS (
  SELECT
    *
  FROM
    augmented
  CROSS JOIN
    UNNEST(
      IF
      -- Provide replacement empty _searches with one null search, to ensure all
      -- clients are included in results
        (
          ARRAY_LENGTH(_searches) > 0,
          _searches,
          [(CAST(NULL AS STRING), CAST(NULL AS STRING), NULL, CAST(NULL AS STRING))]
        )
    )
),
-- Get count based on search type
counted AS (
  SELECT
    ROW_NUMBER() OVER w1 AS rn,
    submission_date,
    client_id,
    engine,
    source,
    country,
    -- GET_SEARCH_ADDON_VERSION(active_addons) AS addon_version,
    app_version,
    distribution_id,
    locale,
    user_pref_browser_search_region,
    -- search_cohort,
    os,
    os_version,
    channel,
    CAST(
      (SELECT key FROM UNNEST(is_default_browser) WHERE key = "true") AS BOOL
    ) AS is_default_browser,
    profile_creation_date,
    default_search_engine,
    default_search_engine_data_load_path,
    default_search_engine_data_submission_url,
    default_private_search_engine,
    default_private_search_engine_data_load_path,
    default_private_search_engine_data_submission_url,
    sample_id,
    SAFE_CAST(subsession_hours_sum AS FLOAT64) AS subsession_hours_sum,
    sessions_started_on_this_day,
    -- active_addons_count_mean,
    scalar_parent_browser_engagement_max_concurrent_tab_count_max AS max_concurrent_tab_count_max,
    scalar_parent_browser_engagement_tab_open_event_count_sum AS tab_open_event_count_sum,
    active_hours_sum,
    scalar_parent_browser_engagement_total_uri_count_sum AS total_uri_count,
    experiments,
    scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
    scalar_parent_urlbar_searchmode_handoff_sum,
    scalar_parent_urlbar_searchmode_keywordoffer_sum,
    scalar_parent_urlbar_searchmode_oneoff_sum,
    scalar_parent_urlbar_searchmode_other_sum,
    scalar_parent_urlbar_searchmode_shortcut_sum,
    scalar_parent_urlbar_searchmode_tabmenu_sum,
    scalar_parent_urlbar_searchmode_tabtosearch_sum,
    scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
    scalar_parent_urlbar_searchmode_topsites_newtab_sum,
    scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
    scalar_parent_urlbar_searchmode_touchbar_sum,
    scalar_parent_urlbar_searchmode_typed_sum,
    profile_age_in_days,
    SUM(IF(type = 'organic', count, 0)) OVER w1 AS organic,
    SUM(IF(type = 'tagged-sap', count, 0)) OVER w1 AS tagged_sap,
    SUM(IF(type = 'tagged-follow-on', count, 0)) OVER w1 AS tagged_follow_on,
    SUM(IF(type = 'sap', count, 0)) OVER w1 AS sap,
    SUM(IF(type = 'ad-click', count, 0)) OVER w1 AS ad_click,
    SUM(IF(type = 'ad-click:organic', count, 0)) OVER w1 AS ad_click_organic,
    SUM(IF(type = 'search-with-ads', count, 0)) OVER w1 AS search_with_ads,
    SUM(IF(type = 'search-with-ads:organic', count, 0)) OVER w1 AS search_with_ads_organic,
    SUM(IF(type = 'unknown', count, 0)) OVER w1 AS unknown,
    CAST(
      NULL AS STRING
    ) AS normalized_engine, -- https://github.com/mozilla/bigquery-etl/issues/2462
    `moz-fx-data-shared-prod.udf.monetized_search`(
      engine,
      country,
      distribution_id,
      submission_date
    ) AS is_sap_monetizable,
    -- has_adblocker_addon,
    policies_is_enterprise,
    CASE
      WHEN mozfun.norm.os(os) = "Windows"
        THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
      ELSE CAST(mozfun.norm.truncate_version(os_version, "major") AS STRING)
    END AS os_version_major,
    CASE
      WHEN mozfun.norm.os(os) = "Windows"
        THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
      ELSE CAST(mozfun.norm.truncate_version(os_version, "minor") AS STRING)
    END AS os_version_minor,
    profile_group_id
  FROM
    flattened
  WHERE
    submission_date = @submission_date
    AND client_id IS NOT NULL
    AND (count < 10000 OR count IS NULL)
  WINDOW
    w1 AS (
      PARTITION BY
        client_id,
        submission_date,
        engine,
        source,
        type
    )
),
search_clients_daily_v9 AS (
  SELECT
    submission_date,
    client_id,
    engine,
    source,
    country,
    -- addon_version,
    app_version,
    distribution_id,
    locale,
    user_pref_browser_search_region,
    -- search_cohort,
    os,
    os_version,
    channel,
    is_default_browser,
    profile_creation_date,
    default_search_engine,
    default_search_engine_data_load_path,
    default_search_engine_data_submission_url,
    default_private_search_engine,
    default_private_search_engine_data_load_path,
    default_private_search_engine_data_submission_url,
    sample_id,
    subsession_hours_sum,
    sessions_started_on_this_day,
    -- active_addons_count_mean,
    max_concurrent_tab_count_max,
    tab_open_event_count_sum,
    active_hours_sum,
    total_uri_count,
    experiments,
    scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
    scalar_parent_urlbar_searchmode_handoff_sum,
    scalar_parent_urlbar_searchmode_keywordoffer_sum,
    scalar_parent_urlbar_searchmode_oneoff_sum,
    scalar_parent_urlbar_searchmode_other_sum,
    scalar_parent_urlbar_searchmode_shortcut_sum,
    scalar_parent_urlbar_searchmode_tabmenu_sum,
    scalar_parent_urlbar_searchmode_tabtosearch_sum,
    scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
    scalar_parent_urlbar_searchmode_topsites_newtab_sum,
    scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
    scalar_parent_urlbar_searchmode_touchbar_sum,
    scalar_parent_urlbar_searchmode_typed_sum,
    profile_age_in_days,
    organic,
    tagged_sap,
    tagged_follow_on,
    sap,
    ad_click,
    ad_click_organic,
    search_with_ads,
    search_with_ads_organic,
    unknown,
    normalized_engine,
    is_sap_monetizable,
    -- has_adblocker_addon,
    policies_is_enterprise,
    os_version_major,
    os_version_minor,
    profile_group_id
  FROM
    counted
  WHERE
    rn = 1
)
SELECT
  *
FROM
  search_clients_daily_v9
