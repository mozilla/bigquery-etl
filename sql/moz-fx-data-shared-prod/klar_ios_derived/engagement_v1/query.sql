-- Query generated via `mobile_kpi_support_metrics` SQL generator.
WITH device_manufacturer_counts AS (
  SELECT
    submission_date,
    device_manufacturer,
    RANK() OVER (PARTITION BY submission_date ORDER BY COUNT(*) DESC) AS manufacturer_rank,
  FROM
    `moz-fx-data-shared-prod.klar_ios.engagement_clients`
  WHERE
    {% if is_init() %}
      submission_date < CURRENT_DATE
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    submission_date,
    device_manufacturer
)
SELECT
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  device_type,
  -- Bucket device manufacturers with low count prior to aggregation
  IF(manufacturer_rank <= 150, device_manufacturer, "other") AS device_manufacturer,
FROM
  `moz-fx-data-shared-prod.klar_ios.engagement_clients`
LEFT JOIN
  device_manufacturer_counts
  USING (submission_date, device_manufacturer)
WHERE
  {% if is_init() %}
    submission_date < CURRENT_DATE
  {% else %}
    submission_date = @submission_date
  {% endif %}
GROUP BY
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  device_type,
  device_manufacturer,
  is_mobile
