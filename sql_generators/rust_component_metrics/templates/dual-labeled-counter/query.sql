SELECT
    DATE(submission_timestamp) AS submission_date,
    "{{ application }}" as application,
    normalized_channel AS channel,
    dual_labeled_counter.key as label1,
    values.key as label2,
    SUM(values.value) as count,
    COUNT(DISTINCT client_info.client_id) as client_count
FROM `moz-fx-data-shared-prod.{{ dataset_name }}.metrics`
    JOIN UNNEST(metrics.dual_labeled_counter.{{ category }}_{{ metric.name }}) as dual_labeled_counter
    JOIN UNNEST(dual_labeled_counter.value) as values
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY 1, 2, 3, 4, 5
