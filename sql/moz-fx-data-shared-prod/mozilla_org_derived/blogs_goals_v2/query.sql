SELECT
  PARSE_DATE('%Y%m%d', event_date) AS `date`,
  user_pseudo_id || '-' || CAST(e.value.int_value AS STRING) AS visit_identifier,
  COUNTIF(event_name = 'download_click') AS downloads,
  COUNTIF(event_name = 'social_share') AS social_share,
  COUNTIF(event_name = 'newsletter_subscribe') AS newsletter_subscription
FROM
  `moz-fx-data-marketing-prod.analytics_314399816.events_*`
JOIN
  UNNEST(event_params) e
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  AND e.key = 'ga_session_id'
  AND e.value.int_value IS NOT NULL
GROUP BY
  `date`,
  visit_identifier
