CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.hang_report_redacted`
AS
SELECT
  document_id,
  submission_timestamp,
  STRUCT(
    client_info.os AS os,
    client_info.os_version AS os_version,
    client_info.architecture AS architecture,
    client_info.app_build AS app_build,
    client_info.client_id AS client_id
  ) AS client_info,
  STRUCT(
    STRUCT(
      metrics.object.hangs_modules AS hangs_modules,
      metrics.object.hangs_reports AS hangs_reports
    ) AS object
  ) AS metrics
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.hang_report_v1`
