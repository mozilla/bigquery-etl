CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.focus_ios_derived.clients_last_seen_joined_v1`
