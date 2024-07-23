-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_attribution_clients`
AS
SELECT
  * EXCEPT (
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    install_source,
    meta_attribution_app,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_install_referrer_response
  ),
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  install_source,
  meta_attribution_app,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_install_referrer_response,
  "fenix" AS product_name,
FROM
  `moz-fx-data-shared-prod.fenix.attribution_clients`
UNION ALL
SELECT
  * EXCEPT (install_source),
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  install_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  "focus_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_android.attribution_clients`
UNION ALL
SELECT
  * EXCEPT (install_source),
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  install_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  "klar_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_android.attribution_clients`
UNION ALL
SELECT
  * EXCEPT (
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    is_suspicious_device_client
  ),
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  is_suspicious_device_client,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  "firefox_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.firefox_ios.attribution_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  "focus_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_ios.attribution_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  "klar_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_ios.attribution_clients`
