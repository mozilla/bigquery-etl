/*
MERGE INTO `moz-fx-data-shared-prod.mozilla_org_derived.blog_performance_v1` T 
USING (
*/

  WITH all_sessions_with_new_events_in_last_3_days AS (
      SELECT DISTINCT
        user_pseudo_id AS ga_client_id,
        CAST(e.value.int_value AS string) AS ga_session_id
      FROM
        `moz-fx-data-marketing-prod.analytics_314399816.events_*`
      JOIN
        UNNEST(event_params) e
      WHERE
        e.key = 'ga_session_id'
        AND e.value.int_value IS NOT NULL
        AND _TABLE_SUFFIX
        BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@submission_date, INTERVAL 3 DAY))
        AND FORMAT_DATE('%Y%m%d', @submission_date)
  ),


--get the unique client IDs from the prior table
    distinct_sessions_last_3_days AS (
      SELECT DISTINCT
        ga_client_id, 
        ga_session_id
      FROM
        all_sessions_with_new_events_in_last_3_days
    ),

all_events_tied_to_distinct_sessions_last_3_days AS (
  
    SELECT 
    a.event_date,
    a.event_timestamp,
    a.event_name,
    a.ga_session_id, 
    a.user_pseudo_id,
    a.page_title,
    a.page_location,
    a.session_engaged_indicator,
    a.key_event 
    FROM ( 
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    event_name,
    CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS STRING
    ) AS ga_session_id,
    user_pseudo_id,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_title'
      LIMIT
        1
    ).string_value AS page_title,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value AS page_location,
    COALESCE(
      (
        SELECT
          ep.value.int_value
        FROM
          UNNEST(event_params) ep
        WHERE
          ep.key = 'session_engaged'
        LIMIT
          1
      ),
      SAFE_CAST(
        (
          SELECT
            ep.value.string_value
          FROM
            UNNEST(event_params) ep
          WHERE
            ep.key = 'session_engaged'
          LIMIT
            1
        ) AS INT64
      )
    ) AS session_engaged_indicator,
    CASE
      WHEN event_name IN (
          'click',
          'cta_click',
          'download_click',
          'newsletter_subscribe',
          'purchase',
          'scroll',
          'social_share'
        )
        THEN 1
      ELSE 0
    END AS key_event
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_2*` 
  ) a 
  JOIN 
  distinct_sessions_last_3_days b
  ON COALESCE(a.ga_session_id, 'UNKNOWN') = COALESCE(b.ga_session_id, 'UNKNOWN')
  AND a.user_pseudo_id = b.ga_client_id
),
-- look only at pageviews on blog.mozilla.org
blog_page_views_by_date_page_title_and_visit_id AS (
  SELECT
    event_date,
    ga_session_id,
    user_pseudo_id,
    page_title,
    COUNT(1) AS nbr_page_views
  FROM
    all_events_tied_to_distinct_sessions_last_3_days
  WHERE
    event_name = 'page_view'
    AND LOWER(page_location) LIKE '%blog.mozilla.org%'
  GROUP BY
    event_date,
    ga_session_id,
    user_pseudo_id,
    page_title
),
-- sessions that had 1 or more blog page view on the submission date
sessions_with_1_or_more_blog_page_view AS (
  SELECT DISTINCT
    user_pseudo_id, ga_session_id
  FROM
    blog_page_views_by_date_page_title_and_visit_id
),
-- session-level engagement flag
session_engagement AS (
  SELECT
    ga_session_id,
    user_pseudo_id,
    MAX(session_engaged_indicator) AS session_engaged_flag
  FROM
    all_events_tied_to_distinct_sessions_last_3_days
  GROUP BY
    ga_session_id,
    user_pseudo_id
),
--get the last non-null page title before each event
key_events_staging AS (
  SELECT
    event_date,
    ga_session_id,
    user_pseudo_id,
    ga_session_id,
    user_pseudo_id,
    event_timestamp,
    event_name,
    key_event,
    LAST_VALUE(page_title IGNORE NULLS) OVER (
      PARTITION BY
        ga_session_id,
        user_pseudo_id
      ORDER BY
        event_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND CURRENT row
    ) AS last_seen_page_title
  FROM
    all_events_tied_to_distinct_sessions_last_3_days
),
--get the # of key events by date, visit ID, and page title on or before the key event
key_events AS (
  SELECT
    event_date,
    last_seen_page_title,
    ga_session_id,
    user_pseudo_id,
    COUNT(
      DISTINCT(event_name || ' - ' || CAST(event_timestamp AS string))
    ) AS nbr_key_events
  FROM
    key_events_staging
  WHERE
    key_event = 1
  GROUP BY
    event_date,
    last_seen_page_title,
    ga_session_id,
    user_pseudo_id
),
stats_by_page_title_and_date AS (
  SELECT
    pv.event_date,
    pv.page_title,
    CONCAT(COALESCE(pv.ga_session_id, 'UNKNOWN_GA_SESSION_ID'), ' - ', pv.user_pseudo_id) AS visit_identifier,
    se.session_engaged_flag, --fix this
    SUM(pv.nbr_page_views),
    SUM(ke.nbr_key_events)
  FROM
    sessions_with_1_or_more_blog_page_view s
  JOIN
    blog_page_views_by_date_page_title_and_visit_id pv
    ON COALESCE(s.ga_session_id, 'UNKNOWN') = COALESCE(pv.ga_session_id, 'UNKNOWN')
    AND s.user_pseudo_id = pv.user_pseudo_id
  LEFT JOIN
    session_engagement se
    ON s.user_pseudo_id = se.user_pseudo_id
    AND COALESCE(s.ga_session_id, 'UNKNOWN') = COALESCE(se.ga_session_id, 'UNKNOWN')
  LEFT JOIN 
  key_events 
  ON pv.event_date= ?
  
  GROUP BY
    1,2,3,4
