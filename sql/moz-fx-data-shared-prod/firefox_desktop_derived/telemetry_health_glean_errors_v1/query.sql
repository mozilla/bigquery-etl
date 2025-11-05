-- Query for telemetry health glean errors
WITH sample AS (
  SELECT
    client_info.client_id AS client_id,
    normalized_channel,
    DATE(submission_timestamp) AS submission_date,
    metrics.labeled_counter.glean_error_invalid_value AS ev,
    metrics.labeled_counter.glean_error_invalid_label AS el,
    metrics.labeled_counter.glean_error_invalid_state AS es,
    metrics.labeled_counter.glean_error_invalid_overflow AS eo
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    sample_id = 0
    AND submission_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
),
-- Denominator: distinct clients per app and day
app_day_totals AS (
  SELECT
    submission_date,
    normalized_channel,
    COUNT(DISTINCT client_id) AS total_clients
  FROM
    sample
  GROUP BY
    submission_date,
    normalized_channel
),
-- Numerator per metric key: distinct clients with any error for that key on that day
metric_clients_by_day AS (
  SELECT
    s.normalized_channel,
    s.submission_date,
    e.key AS metric_key,
    COUNT(DISTINCT s.client_id) AS clients_with_error
  FROM
    sample AS s
  JOIN
    UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
  WHERE
    NOT STARTS_WITH(e.key, 'glean')
    AND NOT STARTS_WITH(e.key, 'fog')
    AND e.value > 0
  GROUP BY
    s.submission_date,
    s.normalized_channel,
    metric_key
)
SELECT
  m.normalized_channel,
  m.submission_date,
  COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
FROM
  metric_clients_by_day AS m
JOIN
  app_day_totals AS t
  USING (submission_date, normalized_channel)
GROUP BY
  m.submission_date,
  m.normalized_channel
