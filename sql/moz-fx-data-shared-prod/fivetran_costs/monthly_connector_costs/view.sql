CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fivetran_costs.monthly_connector_costs`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_connector_costs_v1`
