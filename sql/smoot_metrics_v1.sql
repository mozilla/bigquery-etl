CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_metrics_v1` AS
--
WITH unioned AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_metrics_raw_v1`,
    UNNEST(metrics)
  --
  UNION ALL
  --
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_1week_raw_v1`,
    UNNEST(metrics)
)
--
SELECT
  date,
  usage,
  metric,
  value,
  * EXCEPT (metrics, date, usage, metric, value)
FROM
  unioned
WHERE
  date >= '2018-01-01'
