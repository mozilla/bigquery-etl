CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_echo_show.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_echo_show_derived.clients_last_seen_joined_v1`
