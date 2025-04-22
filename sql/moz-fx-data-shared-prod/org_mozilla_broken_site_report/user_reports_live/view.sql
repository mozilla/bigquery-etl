CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports_live`
AS
WITH live_reports AS (
  SELECT
    *,
    "Firefox" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2023-11-01"
  UNION ALL
  SELECT
    *,
    "Fenix" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2025-01-01"
  UNION ALL
  SELECT
    *,
    "Fenix" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2025-01-01"
  UNION ALL
  SELECT
    *,
    "Fenix" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2025-01-01"
  UNION ALL
  SELECT
    *,
    "Fenix" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2025-01-01"
  UNION ALL
  SELECT
    *,
    "Fenix" AS app_name,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ASC) AS rn
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_live.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2025-01-01"
)
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
  metrics AS details
FROM
  live_reports
WHERE
  rn = 1
ORDER BY
  submission_timestamp ASC
