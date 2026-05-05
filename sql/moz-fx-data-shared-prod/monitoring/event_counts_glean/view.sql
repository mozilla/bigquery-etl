CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.event_counts_glean`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.event_counts_glean_v1`
