CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));

WITH
  -- normalize client_id and rank by document_id
  numbered_duplicates AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id, submission_date_s3, document_id ORDER BY `timestamp` ASC) AS _n,
    * REPLACE(LOWER(client_id) AS client_id)
  FROM
    main_summary_v4
  WHERE
    submission_date_s3 = @submission_date
    AND client_id IS NOT NULL ),
  -- Deduplicating on document_id is necessary to get valid SUM values.
  deduplicated AS (
  SELECT
    * EXCEPT (_n)
  FROM
    numbered_duplicates
  WHERE
    _n = 1 ),
  summary_addon_version AS (
  SELECT
    *,
    udf_mode_last(ARRAY(
    SELECT
      element.version
    FROM
      UNNEST(active_addons.list)
    WHERE
      element.addon_id = 'followonsearch@mozilla.com')) AS addon_version
  FROM
    deduplicated
  ),
  augmented AS (
  SELECT
    *,
    ARRAY_CONCAT(
      ARRAY(
        SELECT
          element.source AS source,
          element.engine AS engine,
          element.count AS count,
          CASE
            WHEN (element.source IN ('searchbar',  'urlbar',  'abouthome',  'newtab',  'contextmenu',  'system',  'activitystream',  'webextension',  'alias') OR element.source IS NULL) THEN 'sap'
            WHEN (STARTS_WITH(element.source, 'in-content:sap:')
            OR STARTS_WITH(element.source, 'sap:')) THEN 'tagged-sap'
            WHEN (STARTS_WITH(element.source, 'in-content:sap-follow-on:') OR STARTS_WITH(element.source,'follow-on:')) THEN 'tagged-follow-on'
            WHEN STARTS_WITH(element.source, 'in-content:organic:') THEN 'organic'
            WHEN STARTS_WITH(element.source, 'ad-click:') THEN 'ad-click'
            WHEN STARTS_WITH(element.source, 'search-with-ads:') THEN 'search-with-ads'
          ELSE
          'unknown'
        END
          AS type
        FROM
          UNNEST(search_counts.list)
      ),
      ARRAY(
        SELECT
          "ad-click:" AS source,
          key AS engine,
          value AS count,
          "ad-click" AS type
        FROM
          UNNEST(scalar_parent_browser_search_ad_clicks.key_value)
      ),
      ARRAY(
        SELECT
          "search-with-ads:" AS source,
          key AS engine,
          value AS count,
          "search-with-ads" AS type
        FROM
          UNNEST(scalar_parent_browser_search_with_ads.key_value)
      )
    ) AS _searches
  FROM
    summary_addon_version
  ),
  -- Aggregate by client_id using windows
  windowed AS (
  SELECT
    ROW_NUMBER() OVER w1_unframed AS _n,
    client_id,
    engine,
    source,
    FIRST_VALUE(country) OVER w1 AS country,
    FIRST_VALUE(app_version) OVER w1 AS app_version,
    FIRST_VALUE(distribution_id) OVER w1 AS distribution_id,
    FIRST_VALUE(locale) OVER w1 AS locale,
    FIRST_VALUE(user_pref_browser_search_region) OVER w1 AS user_pref_browser_search_region,
    FIRST_VALUE(search_cohort) OVER w1 AS search_cohort,
    FIRST_VALUE(addon_version) OVER w1 as addon_version,
    FIRST_VALUE(os) OVER w1 AS os,
    FIRST_VALUE(os_version) OVER w1 AS os_version,
    FIRST_VALUE(channel) OVER w1 AS channel,
    FIRST_VALUE(profile_creation_date) OVER w1 AS profile_creation_date,
    FIRST_VALUE(default_search_engine) OVER w1 AS default_search_engine,
    FIRST_VALUE(default_search_engine_data_load_path) OVER w1 AS default_search_engine_data_load_path,
    FIRST_VALUE(default_search_engine_data_submission_url) OVER w1 AS default_search_engine_data_submission_url,
    FIRST_VALUE(sample_id) OVER w1 AS sample_id,
    COUNTIF(subsession_counter = 1) OVER w1 AS sessions_started_on_this_day,
    UNIX_DATE(DATE(SAFE.TIMESTAMP(subsession_start_date))) - profile_creation_date AS profile_age_in_days,
    SUM(subsession_length/NUMERIC '3600') OVER w1 AS subsession_hours_sum,
    AVG(active_addons_count) OVER w1 AS active_addons_count_mean,
    MAX(scalar_parent_browser_engagement_max_concurrent_tab_count) OVER w1 AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    SUM(scalar_parent_browser_engagement_tab_open_event_count) OVER w1 AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(active_ticks/(3600/5)) OVER w1 AS active_hours_sum,
    NULLIF(SUM(IF(type = 'organic', count, 0)) OVER w1, 0) AS organic,
    NULLIF(SUM(IF(type = 'tagged-sap', count, 0)) OVER w1, 0) AS tagged_sap,
    NULLIF(SUM(IF(type = 'tagged-follow-on', count, 0)) OVER w1, 0) AS tagged_follow_on,
    NULLIF(SUM(IF(type = 'sap', count, 0)) OVER w1, 0) AS sap,
    NULLIF(SUM(IF(type = 'ad-click', count, 0)) OVER w1, 0) AS ad_click,
    NULLIF(SUM(IF(type = 'search-with-ads', count, 0)) OVER w1, 0) AS search_with_ads,
    NULLIF(SUM(IF(type = 'unknown', count, 0)) OVER w1, 0) AS unknown

  FROM
    augmented,
    UNNEST(_searches)
  WHERE
    count < 10000
    AND engine IS NOT NULL
  WINDOW
    -- Aggregations require a framed window
    w1 AS (
    PARTITION BY
      client_id,
      submission_date_s3,
      engine,
      source
    ORDER BY
      `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING),
    -- ROW_NUMBER does not work on a framed window
    w1_unframed AS (
    PARTITION BY
      client_id,
      submission_date_s3,
      engine,
      source
    ORDER BY
      `timestamp` ASC) )
SELECT
  @submission_date AS submission_date,
  * EXCEPT(_n)
FROM
  windowed
WHERE
  _n = 1
