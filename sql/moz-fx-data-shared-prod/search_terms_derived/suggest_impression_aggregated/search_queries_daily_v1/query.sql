SELECT
    DATE(submission_timestamp) as submission_date,
    search_query,
    COUNT(*) AS impressions,
    SUM(CASE WHEN is_clicked = TRUE THEN 1 ELSE 0 END) AS clicks,
    COUNT(DISTINCT context_id) as client_days
FROM
    `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
GROUP BY
    1,
    2
HAVING client_days > 30000
