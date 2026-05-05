CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_targets`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_targets_joined_v1`
