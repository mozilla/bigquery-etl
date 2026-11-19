CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.wwww_site_metrics_summary`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.wwww_site_metrics_summary_v1`
WHERE
  -- Excluding events from the testing phase.
  `date` >= "2025-07-16"
