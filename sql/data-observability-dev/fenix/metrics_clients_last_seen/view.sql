CREATE OR REPLACE VIEW
  `data-observability-dev.fenix.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `data-observability-dev.fenix_derived.metrics_clients_last_seen_v1`
