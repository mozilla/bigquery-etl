SELECT
  IF(COUNT(*) = 0, ERROR(CONCAT('No data for given date ', @submission_date)), 1)
FROM
  `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
