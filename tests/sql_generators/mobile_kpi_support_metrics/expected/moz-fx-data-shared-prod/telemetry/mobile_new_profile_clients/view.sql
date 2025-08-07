-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_new_profile_clients`
AS
SELECT
  * EXCEPT (
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_timestamp,
    play_store_attribution_content,
    play_store_attribution_term,
    play_store_attribution_install_referrer_response,
    meta_attribution_app,
    meta_attribution_timestamp,
    install_source,
    distribution_id
  ),
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  adjust_attribution_timestamp,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_timestamp,
  play_store_attribution_content,
  play_store_attribution_term,
  play_store_attribution_install_referrer_response,
  meta_attribution_app,
  meta_attribution_timestamp,
  install_source,
  distribution_id,
  "fenix" AS product_name,
FROM
  `moz-fx-data-shared-prod.fenix.new_profile_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS TIMESTAMP) AS adjust_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS TIMESTAMP) AS play_store_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_content,
  CAST(NULL AS STRING) AS play_store_attribution_term,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS TIMESTAMP) AS meta_attribution_timestamp,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS distribution_id,
  "focus_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_android.new_profile_clients`
UNION ALL
SELECT
  *,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
  CAST(NULL AS STRING) AS adjust_ad_group,
  CAST(NULL AS STRING) AS adjust_campaign,
  CAST(NULL AS STRING) AS adjust_creative,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS TIMESTAMP) AS adjust_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS TIMESTAMP) AS play_store_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_content,
  CAST(NULL AS STRING) AS play_store_attribution_term,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS TIMESTAMP) AS meta_attribution_timestamp,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS distribution_id,
  "klar_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_android.new_profile_clients`
UNION ALL
SELECT
  * EXCEPT (
    is_suspicious_device_client,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    adjust_attribution_timestamp
  ),
  is_suspicious_device_client,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  adjust_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS TIMESTAMP) AS play_store_attribution_timestamp,
  CAST(NULL AS STRING) AS play_store_attribution_content,
  CAST(NULL AS STRING) AS play_store_attribution_term,
  CAST(NULL AS STRING) AS play_store_attribution_install_referrer_response,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS TIMESTAMP) AS meta_attribution_timestamp,
  CAST(NULL AS STRING) AS install_source,
  CAST(NULL AS STRING) AS distribution_id,
  "firefox_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.firefox_ios.new_profile_clients`
