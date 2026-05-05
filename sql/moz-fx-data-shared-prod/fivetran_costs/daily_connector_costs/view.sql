CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fivetran_costs.daily_connector_costs`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fivetran_costs_derived.daily_connector_costs_v1`
