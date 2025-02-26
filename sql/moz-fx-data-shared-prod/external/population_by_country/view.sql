CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.population_by_country`
AS
SELECT
  time_label AS `year`,
  `location` AS country_name,
  iso2_country_code,
  `value` AS population
FROM
  `moz-fx-data-shared-prod.external_derived.population_v1`
WHERE
  variant_id = 4 --Median population
  AND sex = 'Both sexes'
