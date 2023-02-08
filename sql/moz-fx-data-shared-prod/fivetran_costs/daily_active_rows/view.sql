CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fivetran_costs.daily_active_rows`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fivetran_costs_derived.daily_active_rows_v1`
