CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.ga.www_site_downloads`
AS
SELECT
  *
FROM
  `moz-fx-data-marketing-prod.ga_derived.www_site_downloads_v2`
WHERE
  `date` >= '2023-10-01' --filter out data earlier since downloads not fully set up before this date
