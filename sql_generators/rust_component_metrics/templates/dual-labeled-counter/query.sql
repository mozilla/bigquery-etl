SELECT
    DATE(submission_timestamp) AS submission_date,
    "{{ application }}" as application,
    normalized_channel AS channel,
    dual_labeled_counter.key as label1,
    dual_labeled_counter2.key as label2,
    SUM(dual_labeled_counter2.value) as count,
    COUNT(DISTINCT client_info.client_id) as client_count
FROM `moz-fx-data-shared-prod.{{ dataset_name }}.metrics`
    JOIN UNNEST(metrics.dual_labeled_counter.{{ category }}_{{ metric.name }}) as dual_labeled_counter
    JOIN UNNEST(dual_labeled_counter.value) as dual_labeled_counter2
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY 
    submission_date,
    application,
    channel,
    label1,
    label2
