CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_health_ind_np_by_install_type`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_health_ind_np_by_install_type_v1`
