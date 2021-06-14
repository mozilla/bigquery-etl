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

-- Bug 1693141: Remove engine suffixes for continuity
CREATE TEMP FUNCTION normalize_engine(engine STRING) AS (
  CASE
  WHEN
    ENDS_WITH(engine, ':organic')
    OR ENDS_WITH(engine, ':sap')
    OR ENDS_WITH(engine, ':sap-follow-on')
  THEN
    SPLIT(engine, ':')[OFFSET(0)]
  ELSE
    engine
  END
);

-- Bug 1693141: add organic suffix to source or type if the engine suffix is "organic"
CREATE TEMP FUNCTION organicize_source_or_type(engine STRING, original STRING) AS (
  CASE
  WHEN
    ENDS_WITH(engine, ':organic')
  THEN
    CASE
    WHEN
      -- For some reason, the ad click source is "ad-click:" but type is "ad-click"
      ENDS_WITH(original, ':')
    THEN
      CONCAT(original, 'organic')
    ELSE
      CONCAT(original, ':organic')
    END
  ELSE
    original
  END
);

WITH augmented AS (
  SELECT
    *,
    ARRAY_CONCAT(
      ARRAY(
        SELECT AS STRUCT
          element.source AS source,
          element.engine AS engine,
          element.count AS count,
          CASE
          WHEN
            (
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
                'urlbar-searchmode'
              )
              OR element.source IS NULL
            )
          THEN
            'sap'
          WHEN
            (STARTS_WITH(element.source, 'in-content:sap:') OR STARTS_WITH(element.source, 'sap:'))
          THEN
            'tagged-sap'
          WHEN
            (
              STARTS_WITH(element.source, 'in-content:sap-follow-on:')
              OR STARTS_WITH(element.source, 'follow-on:')
            )
          THEN
            'tagged-follow-on'
          WHEN
            STARTS_WITH(element.source, 'in-content:organic:')
          THEN
            'organic'
          WHEN
            STARTS_WITH(element.source, 'ad-click:')
          THEN
            'ad-click'
          WHEN
            STARTS_WITH(element.source, 'search-with-ads:')
          THEN
            'search-with-ads'
          ELSE
            'unknown'
          END
          AS type
        FROM
          UNNEST(search_counts) AS element
      ),
      ARRAY(
        SELECT AS STRUCT
          organicize_source_or_type(key, "ad-click:") AS source,
          normalize_engine(key) AS engine,
          value AS count,
          organicize_source_or_type(key, "ad-click") AS type
        FROM
          UNNEST(ad_clicks)
      ),
      ARRAY(
        SELECT AS STRUCT
          organicize_source_or_type(key, "search-with-ads:") AS source,
          normalize_engine(key) AS engine,
          value AS count,
          organicize_source_or_type(key, "search-with-ads") AS type
        FROM
          UNNEST(search_with_ads)
      )
    ) AS _searches,
  FROM
    mozdata.tmp.clients_daily_with_search
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
    app_version,
    distribution_id,
    locale,
    user_pref_browser_search_region,
    search_cohort,
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
  * EXCEPT (_n)
FROM
  counted
WHERE
  _n = 1
