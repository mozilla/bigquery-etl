SELECT
  PARSE_DATE('%Y%m%d', event_date) AS `date`,
  user_pseudo_id || '-' || CAST(
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
  ) AS visit_identifier,
  COUNTIF(event_name = 'download_click') AS downloads,
  COUNTIF(event_name = 'social_share') AS share,
  COUNTIF(event_name = 'newsletter_subscribe') AS newsletter_subscription
FROM
  `moz-fx-data-marketing-prod.analytics_314399816.events_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
GROUP BY
  `date`,
  visit_identifier
