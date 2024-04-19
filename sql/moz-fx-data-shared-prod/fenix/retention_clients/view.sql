CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.retention_clients`
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
    `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    channel,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    install_source,
  FROM
    `moz-fx-data-shared-prod.fenix.firefox_android_clients`
)
SELECT
  clients_last_seen.submission_date AS submission_date,
  clients_daily.submission_date AS metric_date,
  clients_daily.first_seen_date,
  clients_last_seen.client_id,
  clients_last_seen.sample_id,
  clients_last_seen.normalized_channel,
  clients_daily.country,
  attribution.install_source,
  CASE
    WHEN clients_daily.isp = 'BrowserStack'
      THEN CONCAT('Firefox Android', ' ', clients_daily.isp)
    ELSE 'Firefox Android'
  END AS app_name,
  -- ping sent retention
  retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
  retention_seen.day_27.active_on_metric_date
  AND retention_seen.day_27.active_in_week_3 AS ping_sent_week_4,
  -- activity retention
  retention_active.day_27.active_on_metric_date AS active_metric_date,
  retention_active.day_27.active_on_metric_date
  AND retention_active.day_27.active_in_week_3 AS retained_week_4,
  -- new client retention
  clients_daily.is_new_profile AS new_client_metric_date,
  clients_daily.is_new_profile
  AND retention_active.day_27.active_in_week_3 AS retained_week_4_new_client,
  clients_daily.is_new_profile
  AND BIT_COUNT(days_active_bits) > 1 AS repeat_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  days_seen_bits,
  days_active_bits,
FROM
  `moz-fx-data-shared-prod.fenix.baseline_clients_daily` AS clients_daily
INNER JOIN
  clients_last_seen
  ON clients_daily.submission_date = clients_last_seen.retention_seen.day_27.metric_date
  AND clients_daily.client_id = clients_last_seen.client_id
  AND clients_daily.normalized_channel = clients_last_seen.normalized_channel
LEFT JOIN
  attribution
  ON clients_daily.client_id = attribution.client_id
  AND clients_daily.sample_id = attribution.sample_id
  AND clients_daily.normalized_channel = attribution.channel
WHERE
  clients_last_seen.retention_seen.day_27.active_on_metric_date
