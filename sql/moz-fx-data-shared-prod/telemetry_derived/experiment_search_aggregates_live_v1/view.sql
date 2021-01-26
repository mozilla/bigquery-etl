CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
UNION ALL
SELECT
  * EXCEPT (timestamp)
FROM
  `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_hourly`
WHERE
  DATE(timestamp) > (
    SELECT
      DATE(MAX(window_end))
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
  )
UNION ALL
SELECT
  * EXCEPT (timestamp)
FROM
  `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_recents`
