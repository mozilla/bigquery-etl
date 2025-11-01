SELECT
  signature,
  AVG(duration_nano) AS average_duration_nano,
  COUNT(*) AS hits
FROM
   `{{ target_project }}.{{ app_id }}.{{ ping_name }}`
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  signature
