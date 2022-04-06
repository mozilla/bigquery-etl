SELECT
  *
FROM
  `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_live
WHERE
  submission_date = @submission_date
