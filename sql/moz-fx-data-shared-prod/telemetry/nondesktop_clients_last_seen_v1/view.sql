CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1`
AS
WITH glean_final AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.fenix_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Lockwise Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Lockwise Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Reference Browser Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'Firefox TV Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'VR Browser Baseline' AS app_name,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser.baseline_clients_last_seen`
),
unioned AS (
  SELECT
    submission_date,
    client_id,
    days_seen_bits,
    days_since_seen,
    days_created_profile_bits,
    days_since_created_profile,
    app_name,
    os,
    osversion AS os_version,
    normalized_channel,
    campaign,
    country,
    locale,
    distribution_id,
    metadata_app_version AS app_version,
    mozfun.norm.product_info(app_name, os) AS pinfo,
  FROM
    `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
  UNION ALL
  SELECT
    submission_date,
    client_id,
    days_seen_bits,
    days_since_seen,
    days_created_profile_bits,
    days_since_created_profile,
    app_name,
    normalized_os AS os,
    normalized_os_version AS os_version,
    normalized_channel,
    NULL AS campaign,
    country,
    locale,
    NULL AS distribution_id,
    app_display_version AS app_version,
    mozfun.norm.product_info(app_name, normalized_os) AS pinfo,
  FROM
    glean_final
)
SELECT
  * EXCEPT (pinfo),
  pinfo.product,
  pinfo.canonical_name,
  pinfo.contributes_to_2019_kpi,
  pinfo.contributes_to_2020_kpi
FROM
  unioned
