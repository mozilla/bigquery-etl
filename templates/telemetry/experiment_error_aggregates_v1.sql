CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.experiment_error_aggregates_v1` AS
SELECT
  submission_date_s3 AS submission_date,
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.experiment_error_aggregates_v1`
