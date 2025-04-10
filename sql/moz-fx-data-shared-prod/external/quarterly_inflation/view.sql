CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.quarterly_inflation`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.external_derived.quarterly_inflation_v1`
