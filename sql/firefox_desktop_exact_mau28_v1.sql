SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  COUNT(DISTINCT client_id) AS mau,
  COUNTIF(submission_date_s3 = @submission_date) AS dau
FROM
  clients_daily_v6
WHERE
  submission_date_s3 <= @submission_date
  AND submission_date_s3 > DATE_SUB(@submission_date, INTERVAL 28 DAY)
