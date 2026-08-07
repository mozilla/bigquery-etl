CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.platform_counts`
AS
SELECT
  app_id,
  stable_trace_id,
  trace_signature,
  app_build,
  normalized_os,
  normalized_os_version,
  architecture,
  SUM(hit_count) AS hit_count
FROM
  (
    SELECT
      "firefox_desktop" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_platform_counts_v1`
    UNION ALL
    SELECT
      "org_mozilla_fenix_nightly" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_platform_counts_v1`
    UNION ALL
    SELECT
      "org_mozilla_firefox_beta" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_platform_counts_v1`
  )
GROUP BY
  app_id,
  stable_trace_id,
  trace_signature,
  app_build,
  normalized_os,
  normalized_os_version,
  architecture
