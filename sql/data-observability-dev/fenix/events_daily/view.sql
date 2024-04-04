CREATE OR REPLACE VIEW
  `data-observability-dev.fenix.events_daily`
AS
SELECT
  *
FROM
  `data-observability-dev.fenix_derived.events_daily_v1`
