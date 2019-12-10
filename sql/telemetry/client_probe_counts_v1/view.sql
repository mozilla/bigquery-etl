CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.client_probe_counts`
AS SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.client_probe_counts_v1`
WHERE metric != "search_counts"
    AND metric NOT LIKE "%browser_search%"
    AND metric NOT LIKE "%browser_engagement_navigation%"