CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_metrics_v1` AS
--
WITH unioned AS (
  SELECT
    date,
    metrics.*,
    raw.* EXCEPT (date, metrics)
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_metrics_raw_v1` AS raw,
    UNNEST(metrics) AS metrics
  --
  UNION ALL
  --
  SELECT
    date,
    metrics.*,
    raw.* EXCEPT (date, metrics)
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_1week_raw_v1` AS raw,
    UNNEST(metrics) AS metrics
)
--
SELECT
  *
FROM
  unioned
WHERE
  date >= '2018-01-01'
