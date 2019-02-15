SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  MAX(submission_date_s3) AS last_seen_date,
  -- approximate LAST_VALUE(input).*
  ARRAY_AGG(input
    ORDER BY submission_date_s3
    DESC LIMIT 1
  )[OFFSET(0)].* EXCEPT (submission_date_s3)
FROM
  clients_daily_v6 AS input
WHERE
  submission_date_s3 <= @submission_date
  AND
  submission_date_s3 > DATE_SUB(@submission_date, INTERVAL 28 DAY)
GROUP BY
  input.client_id
