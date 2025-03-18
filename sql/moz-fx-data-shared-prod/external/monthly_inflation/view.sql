CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.monthly_inflation`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.external_derived.monthly_inflation_v1`
