CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_echo_show.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_echo_show_derived.metrics_clients_daily_v1`
