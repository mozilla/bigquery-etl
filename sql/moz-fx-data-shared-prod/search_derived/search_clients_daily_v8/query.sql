-- Return the version of the search addon if it exists, null otherwise
CREATE TEMP FUNCTION get_search_addon_version(active_addons ANY type) AS (
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
CREATE TEMP FUNCTION add_access_point(
  entries ARRAY<STRUCT<key STRING, value INT64>>,
  access_point STRING
) AS (
  ARRAY(SELECT AS STRUCT CONCAT(key, '.', access_point) AS key, value, FROM UNNEST(entries))
);

-- List of Ad Blocking Addons produced using this logic: https://github.com/mozilla/search-adhoc-analysis/tree/master/monetization-blocking-addons
WITH adblocker_addons AS (
  SELECT
    addon_id,
    addon_name
  FROM
    revenue.monetization_blocking_addons
  WHERE
    blocks_monetization
),
clients_with_adblocker_addons AS (
  SELECT
    client_id,
    submission_date,
    TRUE AS has_adblocker_addon
  FROM
    telemetry.clients_daily
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
combined_access_point AS (
  SELECT
    * EXCEPT (has_adblocker_addon),
    COALESCE(has_adblocker_addon, FALSE) AS has_adblocker_addon,
    ARRAY_CONCAT(
      add_access_point(search_content_urlbar_sum, 'urlbar'),
      add_access_point(search_content_urlbar_persisted_sum, 'urlbar_persisted'),
      add_access_point(search_content_urlbar_handoff_sum, 'urlbar_handoff'),
      add_access_point(search_content_urlbar_searchmode_sum, 'urlbar_searchmode'),
      add_access_point(search_content_contextmenu_sum, 'contextmenu'),
      add_access_point(search_content_about_home_sum, 'about_home'),
      add_access_point(search_content_about_newtab_sum, 'about_newtab'),
      add_access_point(search_content_searchbar_sum, 'searchbar'),
      add_access_point(search_content_system_sum, 'system'),
      add_access_point(search_content_webextension_sum, 'webextension'),
      add_access_point(search_content_tabhistory_sum, 'tabhistory'),
      add_access_point(search_content_reload_sum, 'reload'),
      add_access_point(search_content_unknown_sum, 'unknown')
    ) AS in_content_with_sap,
    ARRAY_CONCAT(
      add_access_point(search_withads_urlbar_sum, 'urlbar'),
      add_access_point(search_withads_urlbar_persisted_sum, 'urlbar_persisted'),
      add_access_point(search_withads_urlbar_handoff_sum, 'urlbar_handoff'),
      add_access_point(search_withads_urlbar_searchmode_sum, 'urlbar_searchmode'),
      add_access_point(search_withads_contextmenu_sum, 'contextmenu'),
      add_access_point(search_withads_about_home_sum, 'about_home'),
      add_access_point(search_withads_about_newtab_sum, 'about_newtab'),
      add_access_point(search_withads_searchbar_sum, 'searchbar'),
      add_access_point(search_withads_system_sum, 'system'),
      add_access_point(search_withads_webextension_sum, 'webextension'),
      add_access_point(search_withads_tabhistory_sum, 'tabhistory'),
      add_access_point(search_withads_reload_sum, 'reload'),
      add_access_point(search_withads_unknown_sum, 'unknown')
    ) AS search_with_ads_with_sap,
    ARRAY_CONCAT(
      add_access_point(search_adclicks_urlbar_sum, 'urlbar'),
      add_access_point(search_adclicks_urlbar_persisted_sum, 'urlbar_persisted'),
      add_access_point(search_adclicks_urlbar_handoff_sum, 'urlbar_handoff'),
      add_access_point(search_adclicks_urlbar_searchmode_sum, 'urlbar_searchmode'),
      add_access_point(search_adclicks_contextmenu_sum, 'contextmenu'),
      add_access_point(search_adclicks_about_home_sum, 'about_home'),
      add_access_point(search_adclicks_about_newtab_sum, 'about_newtab'),
      add_access_point(search_adclicks_searchbar_sum, 'searchbar'),
      add_access_point(search_adclicks_system_sum, 'system'),
      add_access_point(search_adclicks_webextension_sum, 'webextension'),
      add_access_point(search_adclicks_tabhistory_sum, 'tabhistory'),
      add_access_point(search_adclicks_reload_sum, 'reload'),
      add_access_point(search_adclicks_unknown_sum, 'unknown')
    ) AS ad_clicks_with_sap,
  FROM
    telemetry.clients_daily
  LEFT JOIN
    clients_with_adblocker_addons
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
          UNNEST(IF(ARRAY_LENGTH(ad_clicks_with_sap) = 0, ad_clicks, ad_clicks_with_sap))
      ),
      ARRAY(
        SELECT AS STRUCT
          CONCAT('search-with-ads:', COALESCE(SPLIT(key, ':')[SAFE_OFFSET(1)], '')) AS source,
          SPLIT(key, ':')[OFFSET(0)] AS engine,
          value AS count,
          CONCAT('search-with-ads', IF(REGEXP_CONTAINS(key, ':organic'), ':organic', '')) AS type
        FROM
          UNNEST(
            IF(
              ARRAY_LENGTH(search_with_ads_with_sap) = 0,
              search_with_ads,
              search_with_ads_with_sap
            )
          )
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
    -- use row number to dedupe over window
    ROW_NUMBER() OVER w1 AS _n,
    submission_date,
    client_id,
    engine,
    source,
    country,
    get_search_addon_version(active_addons) AS addon_version,
    has_adblocker_addon,
    app_version,
    distribution_id,
    locale,
    user_pref_browser_search_region,
    search_cohort,
    os,
    os_version,
    channel,
    is_default_browser,
    UNIX_DATE(DATE(profile_creation_date)) AS profile_creation_date,
    default_search_engine,
    default_search_engine_data_load_path,
    default_search_engine_data_submission_url,
    default_private_search_engine,
    default_private_search_engine_data_load_path,
    default_private_search_engine_data_submission_url,
    sample_id,
    SAFE_CAST(subsession_hours_sum AS FLOAT64) AS subsession_hours_sum,
    sessions_started_on_this_day,
    active_addons_count_mean,
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
    CAST(
      NULL AS STRING
    ) AS normalized_engine, -- https://github.com/mozilla/bigquery-etl/issues/2462
    SUM(IF(type = 'organic', count, 0)) OVER w1 AS organic,
    SUM(IF(type = 'tagged-sap', count, 0)) OVER w1 AS tagged_sap,
    SUM(IF(type = 'tagged-follow-on', count, 0)) OVER w1 AS tagged_follow_on,
    SUM(IF(type = 'sap', count, 0)) OVER w1 AS sap,
    SUM(IF(type = 'ad-click', count, 0)) OVER w1 AS ad_click,
    SUM(IF(type = 'ad-click:organic', count, 0)) OVER w1 AS ad_click_organic,
    SUM(IF(type = 'search-with-ads', count, 0)) OVER w1 AS search_with_ads,
    SUM(IF(type = 'search-with-ads:organic', count, 0)) OVER w1 AS search_with_ads_organic,
    SUM(IF(type = 'unknown', count, 0)) OVER w1 AS unknown,
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
)
SELECT
  * EXCEPT (_n),
  `moz-fx-data-shared-prod.udf.monetized_search`(
    engine,
    country,
    distribution_id,
    submission_date
  ) AS is_sap_monetizable
FROM
  counted
WHERE
  _n = 1
