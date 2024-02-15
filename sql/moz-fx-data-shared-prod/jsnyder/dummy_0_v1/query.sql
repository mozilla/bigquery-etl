-- Query for jsnyder.dummy_0_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  id,
  application.architecture,
  application.build_id
FROM
  `mozdata.telemetry.main`
WHERE
  DATE(submission_timestamp) = @submission_date
LIMIT
  100
