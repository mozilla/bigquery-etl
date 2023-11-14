CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.event_monitoring_live`
AS
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_update_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.pine_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_reference_browser_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_tv_firefox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.burnham_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.mozphab_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_connect_firefox_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.mozilla_mach_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_klar_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_bergamot_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.firefox_translations_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.mozillavpn_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.glean_dictionary_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.mdn_yari_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.bedrock_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.viu_politica_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.treeherder_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.accounts_frontend_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.monitor_cirrus_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.debug_ping_view_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.monitor_frontend_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.moso_mastodon_backend_derived.event_monitoring_live_v1`
WHERE
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  experiment,
  experiment_branch,
  total_events
FROM
  `moz-fx-data-shared-prod.monitoring_derived.event_monitoring_aggregates_v1`
WHERE
  submission_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
