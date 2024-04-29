CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.retention_clients`
AS
WITH clients_last_seen AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_seen,
    mozfun.bits28.retention(days_active_bits & days_seen_bits, submission_date) AS retention_active,
    days_seen_bits,
    days_active_bits,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_last_seen`
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
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
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
  clients_daily.app_display_version,
  clients_daily.locale,
  (
    clients_daily.app_display_version = '107.2'
    AND clients_daily.submission_date >= '2023-02-01'
  ) AS is_suspicious_device_client,
  -- ping sent retention
  clients_last_seen.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
  clients_last_seen.retention_seen.day_27.active_on_metric_date
  AND clients_last_seen.retention_seen.day_27.active_in_week_3 AS ping_sent_week_4,
  -- activity retention
  clients_last_seen.retention_active.day_27.active_on_metric_date AS active_metric_date,
  clients_last_seen.retention_active.day_27.active_on_metric_date
  AND clients_last_seen.retention_active.day_27.active_in_week_3 AS retained_week_4,
  -- new client retention
  clients_daily.is_new_profile AS new_profile_metric_date,
  (
    clients_daily.is_new_profile
    AND clients_last_seen.retention_active.day_27.active_in_week_3
  ) AS retained_week_4_new_profile,
  (
    clients_daily.is_new_profile
    AND BIT_COUNT(clients_last_seen.days_active_bits) > 1
  ) AS repeat_client,
  clients_daily.adjust_ad_group,
  clients_daily.adjust_campaign,
  clients_daily.adjust_creative,
  clients_daily.adjust_network,
  clients_last_seen.days_seen_bits,
  clients_last_seen.days_active_bits,
FROM
  `moz-fx-data-shared-prod.firefox_ios.baseline_clients_daily` AS clients_daily
INNER JOIN
  clients_last_seen
  ON clients_daily.submission_date = clients_last_seen.retention_seen.day_27.metric_date
  AND clients_daily.client_id = clients_last_seen.client_id
  AND clients_daily.normalized_channel = clients_last_seen.normalized_channel
LEFT JOIN
  attribution
  USING (client_id, normalized_channel)
WHERE
  clients_last_seen.retention_seen.day_27.active_on_metric_date
