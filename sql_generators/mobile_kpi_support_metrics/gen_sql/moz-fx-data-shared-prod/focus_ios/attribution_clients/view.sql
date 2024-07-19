-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.attribution_clients`
AS
SELECT
  client_id,
  sample_id,
  "Organic" AS paid_vs_organic,
FROM
  `moz-fx-data-shared-prod.focus_ios.attribution_clients_v1`
