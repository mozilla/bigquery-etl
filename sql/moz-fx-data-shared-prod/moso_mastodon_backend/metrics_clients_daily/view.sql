CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.moso_mastodon_backend.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.moso_mastodon_backend_derived.metrics_clients_daily_v1`
