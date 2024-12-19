CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_broken_site_report.broken_site_root_domain_daily_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_broken_site_report_derived.broken_site_root_domain_daily_aggregates_v1`
