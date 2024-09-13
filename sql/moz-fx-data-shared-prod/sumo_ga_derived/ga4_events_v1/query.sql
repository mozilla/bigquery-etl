SELECT
  ga.*,
  @submission_date AS submission_date
FROM
  `moz-fx-data-marketing-prod.analytics_314403930.events_*` ga
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
