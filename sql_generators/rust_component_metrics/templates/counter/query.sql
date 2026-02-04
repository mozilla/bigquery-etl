SELECT
    DATE(submission_timestamp) AS submission_date,
    "{{ application }}" as application,
    SUM(metrics.counter.{{ category }}_{{ metric.name }}) as count
FROM `mozdata.{{ dataset_name }}.metrics`
WHERE
  DATE(submission_timestamp) = @submission_date AND
  metrics.counter.{{ category }}_{{ metric.name }} IS NOT NULL
GROUP BY 1, 2
