CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.metrics_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.metrics_daily_v1`
