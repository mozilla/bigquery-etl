WITH input AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date_s3 DESC) AS n,
    *
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 <= @submission_date
    AND submission_date_s3 > DATE_SUB(@submission_date, INTERVAL 28 DAY)
)
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  submission_date_s3 AS last_seen_date,
  * EXCEPT (n,
    submission_date_s3)
FROM
  input
WHERE
  n = 1
