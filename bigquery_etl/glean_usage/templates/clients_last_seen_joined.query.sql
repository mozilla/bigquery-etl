WITH baseline AS (
  SELECT 
    *
  FROM 
    `{{ project_id }}.{{ app_name }}.baseline_clients_last_seen` 
  WHERE 
    submission_date = @submission_date
),
metrics AS (
  SELECT 
    * 
  FROM 
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
  WHERE 
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
)
SELECT 
  baseline.submission_date, 
  * EXCEPT(submission_date) 
FROM
  baseline 
LEFT JOIN metrics
USING (client_id, sample_id)
