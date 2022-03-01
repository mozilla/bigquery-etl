CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mach_derived.clients_last_seen_joined_v1`
