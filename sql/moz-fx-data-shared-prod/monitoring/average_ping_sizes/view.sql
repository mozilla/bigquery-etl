CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.average_ping_sizes`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.average_ping_sizes_v1`
