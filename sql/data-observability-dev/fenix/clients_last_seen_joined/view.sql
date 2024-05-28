CREATE OR REPLACE VIEW
  `data-observability-dev.fenix.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `data-observability-dev.fenix_derived.clients_last_seen_joined_v1`
