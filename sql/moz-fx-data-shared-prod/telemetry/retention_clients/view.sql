CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.retention_clients`
AS
WITH clients_last_seen AS (
  SELECT
    cls.submission_date,
    cls.client_id,
    cls.sample_id,
    cls.normalized_channel,
    mozfun.norm.os(cls.os) AS normalized_os,
    COALESCE(
      mozfun.norm.windows_version_info(cls.os, cls.os_version, cls.windows_build_number),
      NULLIF(SPLIT(cls.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
    ) AS normalized_os_version,
    cls.country,
    cls.is_dau,
    cls.is_wau,
    cls.is_mau,
    mozfun.bits28.retention(cls.days_seen_bits, cls.submission_date) AS retention_seen,
    mozfun.bits28.retention(
      cls.days_active_bits & cls.days_seen_bits,
      cls.submission_date
    ) AS retention_active,
    cls.days_seen_bits,
    cls.days_active_bits,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2` cls
  WHERE
    cls.submission_date = @submission_date
),
new_profiles AS (
  SELECT
    client_id,
    sample_id,
    first_seen_date,
    country,
    locale,
    app_version,
    attribution_campaign,
    attribution_content,
    attribution_dlsource,
    attribution_medium,
    attribution_ua,
    distribution_id,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    TRUE AS is_new_profile
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
),
clients_data AS (
  SELECT
    cls.submission_date AS submission_date,
    cd.submission_date AS metric_date,
    cd.first_seen_date,
    cd.client_id,
    cd.sample_id,
    cd.normalized_channel,
    cd.country,
    cd.app_version,
    cd.locale,
    cd.attribution.campaign AS attribution_campaign,
    cd.attribution.content AS attribution_content,
    cd.attribution.dlsource AS attribution_dlsource,
    cd.attribution.medium AS attribution_medium,
    cd.attribution.ua AS attribution_ua,
    cd.distribution_id AS distribution_id,
    cls.days_seen_bits,
    cls.days_active_bits,
    mozfun.norm.os(cd.os) AS normalized_os,
    cd.normalized_os_version AS normalized_os_version,
    cls.retention_seen.day_27.active_in_week_3 AS retention_active_in_week_3,
  -- ping sent retention
    cls.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
    (
      cls.retention_seen.day_27.active_on_metric_date
      AND cls.retention_seen.day_27.active_in_week_3
    ) AS ping_sent_week_4,
  -- activity retention
    cls.retention_active.day_27.active_on_metric_date AS active_metric_date,
    (
      cls.retention_active.day_27.active_on_metric_date
      AND cls.retention_active.day_27.active_in_week_3
    ) AS retained_week_4,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily` AS cd
  INNER JOIN
    clients_last_seen AS cls
    ON cd.submission_date = cls.retention_seen.day_27.metric_date
    AND cd.client_id = cls.client_id
    AND cd.normalized_channel = cls.normalized_channel
  WHERE
    cls.retention_seen.day_27.active_on_metric_date
    AND cd.submission_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
  -- new profile retention
SELECT
  cd.client_id,
  cd.sample_id,
  cd.submission_date,
  COALESCE(cd.metric_date, np.first_seen_date) AS metric_date,
  COALESCE(cd.country, np.country) AS country,
  COALESCE(cd.locale, np.locale) AS locale,
  COALESCE(cd.app_version, np.app_version) AS app_version,
  COALESCE(cd.normalized_channel, np.normalized_channel) AS normalized_channel,
  COALESCE(cd.first_seen_date, np.first_seen_date) AS first_seen_date,
  COALESCE(cd.attribution_campaign, np.attribution_campaign) AS attribution_campaign,
  COALESCE(cd.attribution_content, np.attribution_content) AS attribution_content,
  COALESCE(cd.attribution_dlsource, np.attribution_dlsource) AS attribution_dlsource,
  COALESCE(cd.attribution_medium, np.attribution_medium) AS attribution_medium,
  COALESCE(cd.attribution_ua, np.attribution_ua) AS attribution_ua,
  COALESCE(cd.normalized_os, np.normalized_os) AS normalized_os,
  COALESCE(cd.normalized_os_version, np.normalized_os_version) AS normalized_os_version,
  cd.ping_sent_metric_date,
  cd.ping_sent_week_4,
  cd.active_metric_date,
  cd.retained_week_4,
  np.is_new_profile,
  (np.is_new_profile AND cd.retention_active_in_week_3) AS retained_week_4_new_profile,
  (np.is_new_profile AND BIT_COUNT(cd.days_active_bits) > 1) AS repeat_profile,
FROM
  clients_data cd
FULL OUTER JOIN
  new_profiles AS np
  ON np.client_id = cd.client_id
  AND np.normalized_channel = cd.normalized_channel
