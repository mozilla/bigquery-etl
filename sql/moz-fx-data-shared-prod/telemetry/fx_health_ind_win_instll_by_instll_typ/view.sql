CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_health_ind_win_instll_by_instll_typ`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_health_ind_win_instll_by_instll_typ_v1`
WHERE
  update_channel = 'release'
