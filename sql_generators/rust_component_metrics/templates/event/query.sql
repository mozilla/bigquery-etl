SELECT
DATE(submission_timestamp) AS submission_date,
"{{ application }}" as application,
COUNT(*) as count
FROM `mozdata.{{ dataset_name }}.events`
CROSS JOIN UNNEST(events) as events 
WHERE
  events.category = "{{ category }}" AND
  events.name = "{{ metric.name }}" AND
  DATE(submission_timestamp) = @submission_date
GROUP BY 1
