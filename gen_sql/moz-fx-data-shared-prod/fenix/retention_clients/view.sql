-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.retention_clients`
AS
WITH active_users AS (
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
    is_mobile,
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    channel AS normalized_channel,
    NULLIF(play_store_attribution_campaign, "") AS play_store_attribution_campaign,
    NULLIF(play_store_attribution_medium, "") AS play_store_attribution_medium,
    NULLIF(play_store_attribution_source, "") AS play_store_attribution_source,
    NULLIF(meta_attribution_app, "") AS meta_attribution_app,
    NULLIF(install_source, "") AS install_source,
    NULLIF(adjust_ad_group, "") AS adjust_ad_group,
    CASE
      WHEN adjust_network IN ('Google Organic Search', 'Organic')
        THEN 'Organic'
      ELSE NULLIF(adjust_campaign, "")
    END AS adjust_campaign,
    NULLIF(adjust_creative, "") AS adjust_creative,
    NULLIF(adjust_network, "") AS adjust_network,
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
)
SELECT
  active_users.submission_date AS submission_date,
  clients_daily.submission_date AS metric_date,
  clients_daily.first_seen_date,
  clients_daily.client_id,
  clients_daily.sample_id,
  active_users.app_name,
  clients_daily.normalized_channel,
  clients_daily.country,
  clients_daily.app_display_version AS app_version,
  clients_daily.locale,
  clients_daily.isp,
  active_users.is_mobile,
  attribution.play_store_attribution_campaign,
  attribution.play_store_attribution_medium,
  attribution.play_store_attribution_source,
  attribution.meta_attribution_app,
  attribution.install_source,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  -- ping sent retention
  active_users.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
  (
    active_users.retention_seen.day_27.active_on_metric_date
    AND active_users.retention_seen.day_27.active_in_week_3
  ) AS ping_sent_week_4,
  -- activity retention
  active_users.retention_active.day_27.active_on_metric_date AS active_metric_date,
  (
    active_users.retention_active.day_27.active_on_metric_date
    AND active_users.retention_active.day_27.active_in_week_3
  ) AS retained_week_4,
  -- new profile retention
  clients_daily.is_new_profile AS new_profile_metric_date,
  (
    clients_daily.is_new_profile
    AND active_users.retention_active.day_27.active_in_week_3
  ) AS retained_week_4_new_profile,
  (
    clients_daily.is_new_profile
    -- Looking back at 27 days to support the official definition of repeat_profile (someone active between days 2 and 28):
    AND BIT_COUNT(mozfun.bits28.range(active_users.days_active_bits, -26, 27)) > 0
  ) AS repeat_profile,
  active_users.days_seen_bits,
  active_users.days_active_bits,
  CASE
    WHEN clients_daily.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
FROM
  `moz-fx-data-shared-prod.fenix.baseline_clients_daily` AS clients_daily
INNER JOIN
  active_users
  ON clients_daily.submission_date = active_users.retention_seen.day_27.metric_date
  AND clients_daily.client_id = active_users.client_id
  AND clients_daily.normalized_channel = active_users.normalized_channel
LEFT JOIN
  attribution
  ON clients_daily.client_id = attribution.client_id
  AND clients_daily.normalized_channel = attribution.normalized_channel
WHERE
  active_users.retention_seen.day_27.active_on_metric_date
