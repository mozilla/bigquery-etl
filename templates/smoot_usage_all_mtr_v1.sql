CREATE OR REPLACE TABLE
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_all_mtr_v1`
PARTITION BY `date` AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_all_v1`
