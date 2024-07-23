-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_engagement`
AS
SELECT
  * EXCEPT (
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    meta_attribution_app,
    install_source,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network
  ),
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  meta_attribution_app,
  install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  "fenix" AS product_name,
FROM
  `moz-fx-data-shared-prod.fenix.engagement`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  "focus_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_android.engagement`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  "klar_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_android.engagement`
UNION ALL
SELECT
  * EXCEPT (
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    is_suspicious_device_client
  ),
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  is_suspicious_device_client,
  "firefox_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.firefox_ios.engagement`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  "focus_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_ios.engagement`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  "klar_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_ios.engagement`
