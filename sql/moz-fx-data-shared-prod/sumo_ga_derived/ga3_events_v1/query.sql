SELECT
  ga.*,
  @submission_date AS submission_date
FROM
  `moz-fx-data-marketing-prod.65912487.ga_sessions_*` ga
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
