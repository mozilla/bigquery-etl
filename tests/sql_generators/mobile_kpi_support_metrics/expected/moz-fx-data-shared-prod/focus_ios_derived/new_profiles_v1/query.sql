-- Query generated via `mobile_kpi_support_metrics` SQL generator.
SELECT
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  os,
  os_version,
  device_manufacturer,
  is_mobile,
  COUNT(*) AS new_profiles,
FROM
  `moz-fx-data-shared-prod.focus_ios.new_profile_clients`
WHERE
  {% if is_init() %}
    first_seen_date < CURRENT_DATE
  {% else %}
    first_seen_date = @submission_date
  {% endif %}
GROUP BY
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  os,
  os_version,
  device_manufacturer,
  is_mobile
