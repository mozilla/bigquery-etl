CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.www_site_downloads`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.www_site_downloads_v3`
WHERE
  `date` >= '2023-10-01' --filter out data earlier since downloads not fully set up before this date
