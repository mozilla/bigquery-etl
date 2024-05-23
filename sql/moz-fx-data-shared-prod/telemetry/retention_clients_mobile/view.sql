-- Query generated via `kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.retention_clients_mobile`
AS
SELECT
  * EXCEPT (
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    meta_attribution_app,
    install_source
  ),
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  meta_attribution_app,
  install_source,
  CAST(NULL AS BOOLEAN) AS is_suspicious_device_client,
FROM
  `moz-fx-data-shared-prod.fenix.retention_clients`
UNION ALL
SELECT
  * EXCEPT (is_suspicious_device_client),
  CAST(NULL AS STRING) AS play_store_attribution_campaign,
  CAST(NULL AS STRING) AS play_store_attribution_medium,
  CAST(NULL AS STRING) AS play_store_attribution_source,
  CAST(NULL AS STRING) AS meta_attribution_app,
  CAST(NULL AS STRING) AS install_source,
  is_suspicious_device_client,
FROM
  `moz-fx-data-shared-prod.firefox_ios.retention_clients`
