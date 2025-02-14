CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports`
AS
SELECT
  document_id AS uuid,
  CAST(submission_timestamp AS DATETIME) AS reported_at,
  metrics.text2.broken_site_report_description AS comments,
  metrics.url2.broken_site_report_url AS url,
  metrics.string.broken_site_report_breakage_category AS breakage_category,
  app_name,
  client_info.app_display_version AS app_version,
  normalized_channel AS app_channel,
  normalized_os AS os,
  metrics as details
FROM
  (
    SELECT
      "Firefox" AS app_name,
      client_info,
      document_id,
      metrics,
      normalized_channel,
      normalized_os,
      submission_timestamp
    FROM
      `moz-fx-data-shared-prod.firefox_desktop.broken_site_report`
    WHERE
      DATE(submission_timestamp) > "2023-11-01"
    UNION ALL
    SELECT
      "Fenix" AS app_name,
      client_info,
      document_id,
      metrics,
      normalized_channel,
      normalized_os,
      submission_timestamp
    FROM
      `moz-fx-data-shared-prod.fenix.broken_site_report`
    WHERE
      DATE(submission_timestamp) > "2025-01-01"
  )
ORDER BY
  submission_timestamp ASC
