CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.events_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.events_daily_v1`
