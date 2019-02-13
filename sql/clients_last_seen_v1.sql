WITH current_sample AS (
  SELECT
    submission_date_s3 AS last_seen_date,
    * EXCEPT (submission_date_s3)
  FROM
    telemetry.clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date
), previous AS (
  SELECT
    * EXCEPT (submission_date,
      generated_time)
  FROM
    analysis.last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND last_seen_date > DATE_SUB(@submission_date, INTERVAL 28 DAY)
)
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  IF(current_sample.client_id IS NOT NULL,
    current_sample,
    previous).*
FROM
  current_sample
FULL JOIN
  previous
USING
  (client_id)
