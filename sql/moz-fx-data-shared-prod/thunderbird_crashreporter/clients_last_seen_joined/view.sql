CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.thunderbird_crashreporter.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.thunderbird_crashreporter_derived.clients_last_seen_joined_v1`
