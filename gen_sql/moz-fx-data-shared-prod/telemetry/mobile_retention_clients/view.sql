-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_retention_clients`
AS
SELECT
  * EXCEPT (
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    meta_attribution_app,
    install_source
  ),
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  meta_attribution_app,
  install_source,
  "fenix" AS product_name,
FROM
  `moz-fx-data-shared-prod.fenix.retention_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  "focus_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_android.retention_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  "klar_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_android.retention_clients`
UNION ALL
SELECT
  * EXCEPT (
    is_suspicious_device_client,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network
  ),
  is_suspicious_device_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  "firefox_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.firefox_ios.retention_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  "focus_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_ios.retention_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  "klar_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_ios.retention_clients`
