CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_fire_tv.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_fire_tv_derived.clients_last_seen_joined_v1`
