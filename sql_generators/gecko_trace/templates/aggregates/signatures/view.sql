CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.signatures` AS
SELECT
  MIN(submission_date) AS submission_date,
  app_id,
  signature,
  AVG(duration_nano) AS average_duration_nano,
  COUNT(*) AS hits
FROM
  `{{ target_project }}.gecko_trace_aggregates.traces`
GROUP BY
  DATE(submission_date),
  app_id,
  signature
