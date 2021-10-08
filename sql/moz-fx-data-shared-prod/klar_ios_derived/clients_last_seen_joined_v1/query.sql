WITH baseline AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.klar_ios.baseline_clients_last_seen`
  WHERE
    submission_date = @submission_date
),
metrics AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.klar_ios.metrics_clients_last_seen`
  WHERE
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
)
SELECT
  baseline.submission_date,
  baseline.normalized_channel,
  * EXCEPT (submission_date, normalized_channel)
FROM
  baseline
LEFT JOIN
  metrics
USING
  (client_id, sample_id)
