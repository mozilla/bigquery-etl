WITH all_submission_date_events AS (
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
    `moz-fx-data-marketing-prod.analytics_314399816.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
-- look only at pageviews on blog.mozilla.org
blog_page_views_by_date_page_title_and_visit_id AS (
  SELECT
    event_date,
    ga_session_id || ' - ' || user_pseudo_id AS visit_identifier,
    page_title,
    COUNT(1) AS nbr_page_views
  FROM
    all_submission_date_events
  WHERE
    event_name = 'page_view'
    AND LOWER(page_location) LIKE '%blog.mozilla.org%'
  GROUP BY
    1,
    2,
    3
),
-- sessions that had 1 or more blog page view on the submission date
sessions_with_1_or_more_blog_page_view AS (
  SELECT DISTINCT
    visit_identifier
  FROM
    blog_page_views_by_date_page_title_and_visit_id
),
-- session-level engagement flag
session_engagement AS (
  SELECT
    ga_session_id || ' - ' || user_pseudo_id AS visit_identifier,
    MAX(session_engaged_indicator) AS session_engaged_flag
  FROM
    all_submission_date_events
  GROUP BY
    1
),
--get the last non-null page title before each event
key_events_staging AS (
  SELECT
    event_date,
    ga_session_id,
    user_pseudo_id,
    ga_session_id || ' - ' || user_pseudo_id AS visit_identifier,
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
    all_submission_date_events
),
--get the # of key events by date, visit ID, and page title on or before the key event
key_events AS (
  SELECT
    event_date,
    last_seen_page_title,
    COUNT(
      DISTINCT(visit_identifier || ' - ' || event_name || ' - ' || CAST(event_timestamp AS string))
    ) AS nbr_key_events
  FROM
    key_events_staging
  WHERE
    key_event = 1
  GROUP BY
    event_date,
    last_seen_page_title
),
stats_by_page_title_and_date AS (
  SELECT
    pv.event_date,
    pv.page_title,
    SUM(pv.nbr_page_views) AS nbr_page_views,
    COUNT(DISTINCT(pv.visit_identifier)) AS nbr_sessions,
    COUNT(
      DISTINCT(CASE WHEN se.session_engaged_flag = 1 THEN pv.visit_identifier ELSE NULL END)
    ) AS nbr_engaged_sessions
  FROM
    sessions_with_1_or_more_blog_page_view s
  JOIN
    blog_page_views_by_date_page_title_and_visit_id pv
    ON s.visit_identifier = pv.visit_identifier
  LEFT JOIN
    session_engagement se
    ON s.visit_identifier = se.visit_identifier
  GROUP BY
    pv.event_date,
    pv.page_title
)
SELECT
  s.event_date,
  s.page_title,
  s.nbr_page_views,
  s.nbr_sessions,
  s.nbr_engaged_sessions,
  ke.nbr_key_events
FROM
  stats_by_page_title_and_date s
LEFT JOIN
  key_events ke
  ON s.page_title = ke.last_seen_page_title
  AND s.event_date = ke.event_date
