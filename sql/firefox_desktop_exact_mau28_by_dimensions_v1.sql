SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  COUNT(*) AS mau,
  COUNTIF(last_seen_date = submission_date) AS dau,
  -- requested fields from bug 1525689
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id
FROM
  clients_last_seen_v1
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id
