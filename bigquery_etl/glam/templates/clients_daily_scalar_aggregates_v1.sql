{{ header }}
WITH extracted AS (
  SELECT
    *,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    "{{ ping_type }}" AS ping_type,
    COALESCE(
      SAFE_CAST(SPLIT(client_info.app_display_version, '.')[OFFSET(0)] AS INT64),
      0
    ) AS app_version,
    client_info.os AS os,
    client_info.app_build AS app_build_id,
    client_info.app_channel AS channel
  FROM
    `moz-fx-data-shared-prod.{{ source_table }}`
  WHERE
    DATE(submission_timestamp) = {{ submission_date }}
    AND client_info.client_id IS NOT NULL
),
sampled_data AS (
  SELECT
    *
  FROM
    extracted
  WHERE
    -- If you're changing this, then you'll also need to change probe_counts_v1,
    -- where sampling is taken into account for counting clients.
    channel IN ("nightly", "beta")
    OR (channel = "release" AND os != "Windows")
    OR (
        channel = "release" AND
        os = "Windows" AND
        sample_id < 10)
),
unlabeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>[
        {{ unlabeled_metrics }}
    ] as scalar_aggregates
  FROM
    sampled_data
  GROUP BY
    {{ attributes }}
),
grouped_labeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<STRUCT<name STRING, type STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>[
        {{ labeled_metrics }}
    ] as metrics
  FROM
    sampled_data
),
flattened_labeled_metrics AS (
  SELECT
    {{ attributes }},
    metrics.name AS metric,
    metrics.type AS metric_type,
    value.key AS key,
    value.value AS value
  FROM
    grouped_labeled_metrics
  CROSS JOIN
    UNNEST(metrics) AS metrics,
    UNNEST(metrics.value) AS value
),
aggregated_labeled_metrics AS (
  SELECT
    {{ attributes }},
    metric,
    metric_type,
    key,
    MAX(value) AS max,
    MIN(value) AS min,
    AVG(value) AS avg,
    SUM(value) AS sum,
    IF(MIN(value) IS NULL, NULL, COUNT(*)) AS count
  FROM
    flattened_labeled_metrics
  GROUP BY
    {{ attributes }},
    metric,
    metric_type,
    key
),
labeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY_CONCAT_AGG(
        ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>[
        (metric, metric_type, key, 'max', max),
        (metric, metric_type, key, 'min', min),
        (metric, metric_type, key, 'avg', avg),
        (metric, metric_type, key, 'sum', sum),
        (metric, metric_type, key, 'count', count)
        ]
    ) AS scalar_aggregates
  FROM
    aggregated_labeled_metrics
  GROUP BY
    {{ attributes }}
)
SELECT
  *
FROM
  unlabeled_metrics
UNION ALL
SELECT
  *
FROM
  labeled_metrics
