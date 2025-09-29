WITH staging AS (
  SELECT
    event_date,
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
    COUNTIF(event_name = 'page_view') AS page_views
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND LOWER(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1).string_value
    ) LIKE '%blog.mozilla.org%'
  GROUP BY
    event_date,
    event_name,
    ga_session_id,
    user_pseudo_id,
    page_title
)
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  page_title,
  COUNTIF(
    event_name IN (
      'click',
      'cta_click',
      'download_click',
      'newsletter_subscribe',
      'scroll',
      'social_share'
    )
  ) AS nbr_key_events,
  SUM(page_views) AS page_views,
  COUNT(DISTINCT(ga_session_id || ' - ' || user_pseudo_id)) AS nbr_sessions
FROM
  staging
GROUP BY
  event_date,
  page_title
