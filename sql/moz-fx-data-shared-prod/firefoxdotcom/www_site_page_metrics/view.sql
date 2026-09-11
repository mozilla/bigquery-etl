CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.www_site_page_metrics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.www_site_page_metrics_v1`
WHERE
  -- Excluding events from the testing phase.
  `date` >= "2025-07-16"
