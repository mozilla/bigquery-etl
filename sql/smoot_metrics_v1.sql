CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_metrics_v1` AS
--
WITH unioned AS (
  SELECT
    date,
    metrics.usage,
    metrics.dau,
    metrics.wau,
    metrics.mau,
    metrics.active_days_in_week,
    NULL AS retained_in_week_1,
    raw.* EXCEPT (date, metrics)
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_metrics_raw_v1` AS raw,
    UNNEST(metrics) AS metrics
  --
  UNION ALL
  --
  SELECT
    date,
    metrics.usage,
    metrics.dau,
    NULL AS wau,
    NULL AS mau,
    NULL AS active_days_in_week,
    NULL AS retained_in_week_1,
    raw.* EXCEPT (date, metrics)
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_1week_raw_v1` AS raw,
    UNNEST(metrics) AS metrics
  --
  UNION ALL
  --
  SELECT
    date,
    metrics.usage,
    NULL AS dau,
    NULL AS wau,
    NULL AS mau,
    NULL AS active_days_in_week,
    metrics.retained_in_week_1,
    raw.* EXCEPT (date, metrics)
  FROM
    `moz-fx-data-derived-datasets.telemetry.smoot_usage_2week_raw_v1` AS raw,
    UNNEST(metrics) AS metrics
)
--
SELECT
  *
FROM
  unioned
WHERE
  date >= '2018-01-01'
