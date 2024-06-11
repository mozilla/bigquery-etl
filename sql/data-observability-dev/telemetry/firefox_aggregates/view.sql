CREATE OR REPLACE VIEW
  `data-observability-dev.telemetry.firefox_aggregates`
AS
SELECT
  first_seen_date,
  channel,
  first_reported_country,
  SUM(client_count) AS client_count,
FROM
  `data-observability-dev.telemetry_derived.firefox_aggregates_v1`
GROUP BY
  first_seen_date,
  channel,
  first_reported_country
