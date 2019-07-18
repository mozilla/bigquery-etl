
--
CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.addons_v2` AS
SELECT
  submission_date_s3 AS submission_date,
  * REPLACE (
    SAFE_CAST(sample_id AS INT64) AS sample_id
  )
FROM
  `moz-fx-data-derived-datasets.telemetry_raw.addons_v2`
