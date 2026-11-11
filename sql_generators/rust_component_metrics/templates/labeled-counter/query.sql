SELECT
    DATE(submission_timestamp) AS submission_date,
    "{{ application }}" as application,
    normalized_channel AS channel,
    labeled_counter.key as label,
    SUM(labeled_counter.value) as count,
    COUNT(DISTINCT client_info.client_id) as client_count
FROM `moz-fx-data-shared-prod.{{ dataset_name }}.metrics`
    JOIN UNNEST(metrics.labeled_counter.{{ category }}_{{ metric.name }}) as labeled_counter
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY 1, 2, 3, 4
