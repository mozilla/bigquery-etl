-- Return the version of the search addon if it exists, null otherwise
CREATE TEMP FUNCTION get_search_addon_version(active_addons ANY type) AS (
  (
    SELECT
      udf.mode_last(ARRAY_AGG(version))
    FROM
      UNNEST(active_addons)
    WHERE
      addon_id = 'followonsearch@mozilla.com'
    GROUP BY
      version
  )
);

WITH overactive AS (
  -- find client_ids with over 200,000 pings in a day
  SELECT
    client_id
  FROM
    telemetry.main_summary
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 200000
),
client_experiments AS (
  SELECT
    client_id,
    udf.map_mode_last(ARRAY_CONCAT_AGG(experiments)) AS experiments,
  FROM
    telemetry.main_summary
  LEFT JOIN
    overactive
  USING
    (client_id)
  WHERE
    submission_date = @submission_date
    AND overactive.client_id IS NULL
  GROUP BY
    client_id
),
augmented AS (
  SELECT
    * EXCEPT (experiments),
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
          "ad-click:" AS source,
          key AS engine,
          value AS count,
          "ad-click" AS type
        FROM
          UNNEST(scalar_parent_browser_search_ad_clicks)
      ),
      ARRAY(
        SELECT AS STRUCT
          "search-with-ads:" AS source,
          key AS engine,
          value AS count,
          "search-with-ads" AS type
        FROM
          UNNEST(scalar_parent_browser_search_with_ads)
      )
    ) AS _searches,
    -- Aggregate numerical values before flattening engine/source array
    SUM(subsession_length / 3600) OVER w1 AS subsession_hours_sum,
    COUNTIF(subsession_counter = 1) OVER w1 AS sessions_started_on_this_day,
    AVG(active_addons_count) OVER w1 AS active_addons_count_mean,
    MAX(
      scalar_parent_browser_engagement_max_concurrent_tab_count
    ) OVER w1 AS max_concurrent_tab_count_max,
    SUM(scalar_parent_browser_engagement_tab_open_event_count) OVER w1 AS tab_open_event_count_sum,
    SUM(active_ticks / (3600 / 5)) OVER w1 AS active_hours_sum,
    SUM(scalar_parent_browser_engagement_total_uri_count) OVER w1 AS total_uri_count,
    COALESCE(client_experiments.experiments, []) AS experiments,
  FROM
    telemetry.main_summary
  LEFT JOIN
    client_experiments
  USING
    (client_id)
  WINDOW
    w1 AS (
      PARTITION BY
        client_id,
        submission_date
    )
),
flattened AS (
  SELECT
    *
  FROM
    augmented,
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
  -- Aggregate by client_id using windows
windowed AS (
  SELECT
    ROW_NUMBER() OVER w1_unframed AS _n,
    submission_date,
    client_id,
    engine,
    source,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(get_search_addon_version(active_addons)) OVER w1) AS addon_version,
    udf.mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf.mode_last(ARRAY_AGG(distribution_id) OVER w1) AS distribution_id,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf.mode_last(
      ARRAY_AGG(user_pref_browser_search_region) OVER w1
    ) AS user_pref_browser_search_region,
    udf.mode_last(ARRAY_AGG(search_cohort) OVER w1) AS search_cohort,
    udf.mode_last(ARRAY_AGG(os) OVER w1) AS os,
    udf.mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf.mode_last(ARRAY_AGG(channel) OVER w1) AS channel,
    udf.mode_last(ARRAY_AGG(is_default_browser) OVER w1) AS is_default_browser,
    udf.mode_last(ARRAY_AGG(profile_creation_date) OVER w1) AS profile_creation_date,
    udf.mode_last(ARRAY_AGG(default_search_engine) OVER w1) AS default_search_engine,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_data_load_path) OVER w1
    ) AS default_search_engine_data_load_path,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_data_submission_url) OVER w1
    ) AS default_search_engine_data_submission_url,
    udf.mode_last(
      ARRAY_AGG(default_private_search_engine) OVER w1
    ) AS default_private_search_engine,
    udf.mode_last(
      ARRAY_AGG(default_private_search_engine_data_load_path) OVER w1
    ) AS default_private_search_engine_data_load_path,
    udf.mode_last(
      ARRAY_AGG(default_private_search_engine_data_submission_url) OVER w1
    ) AS default_private_search_engine_data_submission_url,
    udf.mode_last(ARRAY_AGG(sample_id) OVER w1) AS sample_id,
    subsession_hours_sum,
    sessions_started_on_this_day,
    active_addons_count_mean,
    max_concurrent_tab_count_max,
    tab_open_event_count_sum,
    active_hours_sum,
    total_uri_count,
    experiments,
    SAFE_SUBTRACT(
      UNIX_DATE(DATE(SAFE.TIMESTAMP(subsession_start_date))),
      profile_creation_date
    ) AS profile_age_in_days,
    SUM(IF(type = 'organic', count, 0)) OVER w1 AS organic,
    SUM(IF(type = 'tagged-sap', count, 0)) OVER w1 AS tagged_sap,
    SUM(IF(type = 'tagged-follow-on', count, 0)) OVER w1 AS tagged_follow_on,
    SUM(IF(type = 'sap', count, 0)) OVER w1 AS sap,
    SUM(IF(type = 'ad-click', count, 0)) OVER w1 AS ad_click,
    SUM(IF(type = 'search-with-ads', count, 0)) OVER w1 AS search_with_ads,
    SUM(IF(type = 'unknown', count, 0)) OVER w1 AS unknown
  FROM
    flattened
  WHERE
    submission_date = @submission_date
    AND client_id IS NOT NULL
    AND (count < 10000 OR count IS NULL)
  WINDOW
    -- Aggregations require a framed window
    w1 AS (
      PARTITION BY
        client_id,
        submission_date,
        engine,
        source,
        type
      ORDER BY
        `timestamp` ASC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- ROW_NUMBER does not work on a framed window
    w1_unframed AS (
      PARTITION BY
        client_id,
        submission_date,
        engine,
        source,
        type
      ORDER BY
        `timestamp` ASC
    )
)
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
