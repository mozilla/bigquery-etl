CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_error_all`
AS
-- We exclude the payload field for views that are accessible to
-- most users.
SELECT
  'structured' AS pipeline_family,
  COALESCE(
    -- glean pings
    JSON_VALUE(`moz-fx-data-shared-prod.udf_js.gunzip`(payload), '$.client_info.app_channel'),
    -- firefox-installer
    JSON_VALUE(`moz-fx-data-shared-prod.udf_js.gunzip`(payload), '$.update_channel')
  ) AS channel,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.structured`
UNION ALL
SELECT
  'stub_installer' AS pipeline_family,
  SPLIT(uri, "/")[SAFE_OFFSET(3)] AS channel,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.stub_installer`
UNION ALL
SELECT
  'telemetry' AS pipeline_family,
  `moz-fx-data-shared-prod.udf.parse_desktop_telemetry_uri`(uri).app_update_channel AS channel,
  * EXCEPT (payload)
FROM
  `moz-fx-data-shared-prod.payload_bytes_error.telemetry`
