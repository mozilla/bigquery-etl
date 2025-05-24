WITH active_users AS (
  SELECT
    au.submission_date,
    au.client_id,
    au.sample_id,
    mozfun.bits28.retention(au.days_seen_bits, au.submission_date) AS retention_seen,
    mozfun.bits28.retention(
      au.days_active_bits & au.days_seen_bits,
      au.submission_date
    ) AS retention_active,
    au.days_seen_bits,
    au.days_active_bits,
    au.is_desktop
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` AS au
  WHERE
    au.submission_date = @submission_date
),
new_profiles AS (
  SELECT
    cfs.client_id,
    cfs.sample_id,
    cfs.profile_group_id,
    cfs.first_seen_date,
    cfs.country,
    cfs.locale,
    cfs.app_display_version AS app_version,
    cfs.attribution.campaign AS attribution_campaign,
    cfs.attribution.content AS attribution_content,
    --cfs.attribution_dlsource AS attribution_dlsource,
    cfs.attribution.source AS attribution_source,
    cfs.attribution.medium AS attribution_medium,
    --cfs.attribution.ua AS attribution_ua,
    --cfs.attribution_experiment,
    --cfs.attribution_variation,
    cfs.distribution_id,
    cfs.is_desktop,
    cfs.isp,
    cfs.normalized_channel,
    --startup_profile_selection_reason, not available yet in cfs
    cfs.normalized_os,
    COALESCE(
      windows_version,
      NULLIF(SPLIT(cfs.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
    ) AS normalized_os_version,
    COALESCE(au.submission_date, DATE_ADD(cfs.first_seen_date, INTERVAL 27 day)) AS submission_date,
    TRUE AS is_new_profile,
    au.retention_active.day_27.active_in_week_3 AS retained_week_4_new_profile,
    BIT_COUNT(
      mozfun.bits28.from_string('0111111111111111111111111111') & au.days_active_bits
    ) > 0 AS repeat_profile
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_first_seen` cfs
  LEFT JOIN
    active_users AS au
    ON cfs.first_seen_date = au.retention_active.day_27.metric_date
    AND cfs.client_id = au.client_id
  WHERE
    cfs.first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND cfs.submission_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
clients_data AS (
  SELECT
    au.submission_date AS submission_date,
    cd.submission_date AS metric_date,
    cd.first_seen_date,
    cd.client_id,
    cd.sample_id,
    cd.profile_group_id,
    cd.normalized_channel,
    cd.country,
    cd.app_display_version AS app_version,
    cd.locale,
    cd.attribution.campaign AS attribution_campaign,
    cd.attribution.content AS attribution_content,
    --cd.attribution_dlsource AS attribution_dlsource,
    cd.attribution.source AS attribution_source,
    cd.attribution.medium AS attribution_medium,
    --cd.attribution.ua AS attribution_ua,
    --cd.attribution.experiment AS attribution_experiment,
    --cd.attribution.variation AS attribution_variation,
    --cd.startup_profile_selection_reason_first AS startup_profile_selection_reason,
    cd.distribution_id AS distribution_id,
    au.is_desktop,
    cd.isp,
    au.days_seen_bits,
    au.days_active_bits,
    cd.normalized_os,
    --check the function below is working properly
    COALESCE(
      mozfun.norm.glean_windows_version_info(
        cd.normalized_os,
        cd.normalized_os_version,
        cd.windows_build_number
      ),
      NULLIF(SPLIT(cd.normalized_os_version, ".")[SAFE_OFFSET(0)], "")
    ) AS normalized_os_version,
    --check if the function above is working properly
    au.retention_seen.day_27.active_in_week_3 AS retention_active_in_week_3,
  -- ping sent retention
    au.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
    (
      au.retention_seen.day_27.active_on_metric_date
      AND au.retention_seen.day_27.active_in_week_3
    ) AS ping_sent_week_4,
  -- activity retention
    au.retention_active.day_27.active_on_metric_date AS active_metric_date,
    (
      au.retention_active.day_27.active_on_metric_date
      AND au.retention_active.day_27.active_in_week_3
    ) AS retained_week_4,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_daily` AS cd
  INNER JOIN
    active_users AS au
    ON cd.submission_date = au.retention_seen.day_27.metric_date
    AND cd.client_id = au.client_id
  WHERE
    au.retention_seen.day_27.active_on_metric_date
    AND cd.submission_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  COALESCE(cd.client_id, np.client_id) AS client_id,
  COALESCE(cd.sample_id, np.sample_id) AS sample_id,
  COALESCE(cd.profile_group_id, np.profile_group_id) AS profile_group_id,
  COALESCE(cd.submission_date, np.submission_date) AS submission_date,
  COALESCE(cd.metric_date, np.first_seen_date) AS metric_date,
  COALESCE(cd.country, np.country) AS country,
  COALESCE(cd.locale, np.locale) AS locale,
  COALESCE(cd.app_version, np.app_version) AS app_version,
  COALESCE(cd.normalized_channel, np.normalized_channel) AS normalized_channel,
  COALESCE(cd.first_seen_date, np.first_seen_date) AS first_seen_date,
  COALESCE(cd.attribution_campaign, np.attribution_campaign) AS attribution_campaign,
  COALESCE(cd.attribution_content, np.attribution_content) AS attribution_content,
  --COALESCE(cd.attribution_dlsource, np.attribution_dlsource) AS attribution_dlsource,
  COALESCE(cd.attribution_source, np.attribution_source) AS attribution_source,
  COALESCE(cd.attribution_medium, np.attribution_medium) AS attribution_medium,
  --COALESCE(cd.attribution_ua, np.attribution_ua) AS attribution_ua,
  --COALESCE(cd.attribution_experiment, np.attribution_experiment) AS attribution_experiment,
  --COALESCE(cd.attribution_variation, np.attribution_variation) AS attribution_variation,
  /*
  COALESCE(
    cd.startup_profile_selection_reason,
    np.startup_profile_selection_reason
  ) AS startup_profile_selection_reason,
  */
  COALESCE(cd.normalized_os, np.normalized_os) AS normalized_os,
  COALESCE(cd.normalized_os_version, np.normalized_os_version) AS normalized_os_version,
  COALESCE(cd.distribution_id, np.distribution_id) AS distribution_id,
  COALESCE(cd.isp, np.isp) AS isp,
  COALESCE(cd.is_desktop, np.is_desktop) AS is_desktop,
  cd.days_seen_bits,
  cd.days_active_bits,
  cd.ping_sent_metric_date,
  cd.ping_sent_week_4,
  cd.active_metric_date,
  cd.retained_week_4,
  COALESCE(np.is_new_profile, FALSE) AS new_profile_metric_date,
  COALESCE(np.repeat_profile, FALSE) AS repeat_profile,
  COALESCE(np.retained_week_4_new_profile, FALSE) AS retained_week_4_new_profile,
FROM
  clients_data cd
FULL OUTER JOIN
  new_profiles AS np
  ON np.client_id = cd.client_id
  AND np.first_seen_date = cd.metric_date
