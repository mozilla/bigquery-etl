CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.retention_clients`
AS
WITH clients_last_seen AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    app_name,
    normalized_channel,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_seen,
    mozfun.bits28.retention(days_active_bits & days_seen_bits, submission_date) AS retention_active,
    days_seen_bits,
    days_active_bits,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_last_seen_extended_activity`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    channel AS normalized_channel,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    is_suspicious_device_client,
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.firefox_ios_clients_v1`
)
SELECT
  clients_last_seen.submission_date AS submission_date,
  clients_daily.submission_date AS metric_date,
  clients_daily.first_seen_date,
  clients_daily.client_id,
  clients_daily.sample_id,
  clients_last_seen.app_name,
  clients_daily.normalized_channel,
  clients_daily.country,
  clients_daily.app_display_version AS app_version,
  clients_daily.locale,
  clients_daily.isp,
  attribution.is_suspicious_device_client,
  -- ping sent retention
  clients_last_seen.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
  (
    clients_last_seen.retention_seen.day_27.active_on_metric_date
    AND clients_last_seen.retention_seen.day_27.active_in_week_3
  ) AS ping_sent_week_4,
  -- activity retention
  clients_last_seen.retention_active.day_27.active_on_metric_date AS active_metric_date,
  (
    clients_last_seen.retention_active.day_27.active_on_metric_date
    AND clients_last_seen.retention_active.day_27.active_in_week_3
  ) AS retained_week_4,
  -- new profile retention
  clients_daily.is_new_profile AS new_profile_metric_date,
  (
    clients_daily.is_new_profile
    AND clients_last_seen.retention_active.day_27.active_in_week_3
  ) AS retained_week_4_new_profile,
  (
    clients_daily.is_new_profile
    -- Looking back at 27 days to support the official definition of repeat_profile (someone active between days 2 and 28):
    AND BIT_COUNT(mozfun.bits28.range(clients_last_seen.days_active_bits, -26, 27)) > 0
  ) AS repeat_profile,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  clients_last_seen.days_seen_bits,
  clients_last_seen.days_active_bits,
  CASE
    WHEN first_seen_date = clients_daily.submission_date
      THEN 'new_profile'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage
FROM
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_daily` AS clients_daily
INNER JOIN
  clients_last_seen
  ON clients_daily.submission_date = clients_last_seen.retention_seen.day_27.metric_date
  AND clients_daily.client_id = clients_last_seen.client_id
  AND clients_daily.normalized_channel = clients_last_seen.normalized_channel
LEFT JOIN
  attribution
  ON clients_daily.client_id = attribution.client_id
  AND clients_daily.normalized_channel = attribution.normalized_channel
WHERE
  clients_last_seen.retention_seen.day_27.active_on_metric_date
