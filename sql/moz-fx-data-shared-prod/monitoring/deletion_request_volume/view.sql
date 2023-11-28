CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.deletion_request_volume`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.deletion_request_volume_v2`
