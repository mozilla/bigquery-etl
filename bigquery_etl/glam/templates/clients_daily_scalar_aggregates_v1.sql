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
    source_metric,
  FROM
    sampled_data
  CROSS JOIN
    UNNEST(
      ARRAY<STRUCT<name STRING, type STRING, value FLOAT64>>[
        {{ unlabeled_metrics }},
        {{ labeled_metrics }}
      ]
    ) AS source_metric
  WHERE source_metric.value IS NOT NULL
),
aggregated_unlabeled_metrics AS (
  SELECT
  {{ attributes }},
  source_metric.name AS metric,
  source_metric.type AS metric_type,
  CASE
      source_metric.type
      WHEN 'boolean'
        THEN ARRAY<STRUCT<key STRING, agg_type STRING, value FLOAT64>>[
            ('', 'false', SUM(CAST(source_metric.value = 0 AS INT64))),
            ('', 'true', SUM(CAST(source_metric.value = 1 AS INT64)))
          ]
      ELSE ARRAY<STRUCT<key STRING, agg_type STRING, value FLOAT64>>[
          ('', 'avg', AVG(CAST(source_metric.value AS NUMERIC))),
          ('', 'count', IF(MIN(source_metric.value) IS NULL, NULL, COUNT(*))),
          ('', 'max', MAX(CAST(source_metric.value AS NUMERIC))),
          ('', 'min', MIN(CAST(source_metric.value AS NUMERIC))),
          ('', 'sum', SUM(CAST(source_metric.value AS NUMERIC)))
        ]
    END AS scalar_aggregates,
  FROM
    unlabeled_metrics
  GROUP BY
    client_id,
    ping_type,
    submission_date,
    os,
    app_version,
    app_build_id,
    channel,
    source_metric.name,
    source_metric.type
),
grouped_labeled_metrics AS (
  SELECT
    {{ attributes }},
    source_metric,
    value,
  FROM
    sampled_data
  CROSS JOIN
    UNNEST(
      ARRAY<STRUCT<name STRING, type STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>[
          {{ labeled_metrics }}
      ]
    ) AS source_metric
  CROSS JOIN
    UNNEST(source_metric.value) AS value
  WHERE
    source_metric.value IS NOT NULL
),
aggregated_labeled_metrics AS (
  SELECT
    {{ attributes }},
    source_metric.name AS metric,
    source_metric.type AS metric_type,
    ARRAY<STRUCT<key STRING, agg_type STRING, value FLOAT64>>[
      (value.key, 'max', MAX(value.value)),
      (value.key, 'min', MIN(value.value)),
      (value.key, 'avg', AVG(value.value)),
      (value.key, 'sum', SUM(value.value)),
      (value.key, 'count', IF(MIN(value.value) IS NULL, NULL, COUNT(*)))
    ] AS scalar_aggregates,
  FROM
    grouped_labeled_metrics
  GROUP BY
    client_id,
    ping_type,
    submission_date,
    os,
    app_version,
    app_build_id,
    channel,
    source_metric.name,
    source_metric.type,
    value.key
)
SELECT
  *
FROM
  aggregated_unlabeled_metrics
UNION ALL
SELECT
  *
FROM
  aggregated_labeled_metrics