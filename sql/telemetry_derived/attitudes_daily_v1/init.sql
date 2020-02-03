CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.attitudes_daily_v1`
PARTITION BY
  submission_date
CLUSTER BY
  question_key
AS
SELECT
  *,
  CAST(NULL AS DATE) AS `date`,
  CAST(NULL AS STRING) AS question,
  CAST(NULL AS STRING) AS question_key,
  CAST(NULL AS STRING) AS value,
  CAST(NULL AS STRING) AS survey_id
FROM
  telemetry.clients_daily
WHERE
  FALSE
