CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_all_v1`
AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_desktop_v1`
--
UNION ALL
--
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_nondesktop_v1`
--
UNION ALL
--
SELECT
  `date`,
  usage,
  id_bucket,
  CAST(NULL AS STRING) AS app_version,
  country,
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS channel,
  * EXCEPT (`date`, usage, id_bucket, country)
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_fxa_v1`
