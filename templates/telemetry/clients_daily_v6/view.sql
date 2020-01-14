CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily_v6`
AS
SELECT
  submission_date AS submission_date_s3,
  * REPLACE (
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    IFNULL(geo_subdivision1, '??') AS geo_subdivision1,
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
