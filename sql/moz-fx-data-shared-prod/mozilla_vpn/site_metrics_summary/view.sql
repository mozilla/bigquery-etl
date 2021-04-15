CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.site_metrics_summary`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.mozilla_vpn_derived.site_metrics_summary_v1
