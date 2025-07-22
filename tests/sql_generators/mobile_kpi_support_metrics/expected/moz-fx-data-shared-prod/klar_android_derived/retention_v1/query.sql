-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH device_manufacturer_counts AS (
  SELECT
    submission_date,
    device_manufacturer,
    RANK() OVER (PARTITION BY submission_date ORDER BY COUNT(*) DESC) AS manufacturer_rank,
  FROM
    `moz-fx-data-shared-prod.klar_android.retention_clients`
  WHERE
    {% if is_init() %}
      metric_date < DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
      AND submission_date < CURRENT_DATE
    {% else %}
      metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
      AND submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    device_manufacturer
)
SELECT
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  is_mobile,
  COUNTIF(ping_sent_metric_date) AS ping_sent_metric_date,
  COUNTIF(ping_sent_week_4) AS ping_sent_week_4,
  COUNTIF(active_metric_date) AS active_metric_date,
  COUNTIF(retained_week_4) AS retained_week_4,
  COUNTIF(retained_week_4_new_profile) AS retained_week_4_new_profiles,
  COUNTIF(new_profile_metric_date) AS new_profiles_metric_date,
  COUNTIF(repeat_profile) AS repeat_profiles,
  device_type,
  -- Bucket device manufacturers with low count prior to aggregation
  IF(manufacturer_rank <= 150, device_manufacturer, "other") AS device_manufacturer,
FROM
  `moz-fx-data-shared-prod.klar_android.retention_clients`
LEFT JOIN
  device_manufacturer_counts
  USING (submission_date, device_manufacturer)
WHERE
  {% if is_init() %}
    metric_date < DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
    AND submission_date < CURRENT_DATE
  {% else %}
    metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND submission_date = @submission_date
  {% endif %}
GROUP BY
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  device_type,
  device_manufacturer,
  is_mobile
