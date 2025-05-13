CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.us_unemployment`
AS
SELECT
  DATE(CAST(Year AS integer), CAST(RIGHT(Period, 2) AS integer), 1) AS PeriodStartDate,
  UnemploymentRate,
  LastUpdated
FROM
  `moz-fx-data-shared-prod.external_derived.us_unemployment_v1`
