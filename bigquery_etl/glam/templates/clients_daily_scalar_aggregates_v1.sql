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
unlabeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>[
      {{ unlabeled_metrics }}
    ] AS scalar_aggregates
  FROM
    extracted
  WHERE
    -- If you're changing this, then you'll also need to change probe_counts_v1,
    -- where sampling is taken into account for counting clients.
    channel IN ("nightly", "beta")
    OR (channel = "release" AND os != "Windows")
    OR (channel = "release" AND os = "Windows" AND sample_id < 10)
  GROUP BY
    {{ attributes }}
),
{% if client_sampled_unlabeled_metrics %}
  sampled_unlabeled_metrics AS (
    SELECT
      {{ attributes }},
      ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>[
        {{ client_sampled_unlabeled_metrics }}
      ] AS scalar_aggregates
    FROM
      extracted
    WHERE
      channel = "{{ client_sampled_channel}}"
      AND os = "{{ client_sampled_os}}"
      AND sample_id = {{ client_sampled_max_sample_id }}
    GROUP BY
      {{ attributes }}
  ),
{% endif %}
unioned_unlabeled_metrics AS (
  SELECT
    *
  FROM
    unlabeled_metrics
    {% if client_sampled_unlabeled_metrics %}
      UNION ALL
      SELECT
        *
      FROM
        sampled_unlabeled_metrics
    {% endif %}
),
grouped_labeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<STRUCT<name STRING, type STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>[
      {{ labeled_metrics }}
    ] AS metrics
  FROM
    extracted
  WHERE
    -- If you're changing this, then you'll also need to change probe_counts_v1,
    -- where sampling is taken into account for counting clients.
    channel IN ("nightly", "beta")
    OR (channel = "release" AND os != "Windows")
    OR (channel = "release" AND os = "Windows" AND sample_id < 10)
),
{% if client_sampled_labeled_metrics %}
  grouped_sampled_labeled_metrics AS (
    SELECT
      {{ attributes }},
      ARRAY<STRUCT<name STRING, type STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>[
        {{ client_sampled_labeled_metrics }}
      ] AS metrics
    FROM
      extracted
    WHERE
      channel = "{{ client_sampled_channel}}"
      AND os = "{{ client_sampled_os}}"
      AND sample_id = {{ client_sampled_max_sample_id }}
  ),
{% endif %}
unioned_grouped_labeled_metrics AS (
  SELECT
    *
  FROM
    grouped_labeled_metrics
    {% if client_sampled_labeled_metrics %}
      UNION ALL
      SELECT
        *
      FROM
        grouped_sampled_labeled_metrics
    {% endif %}
),
flattened_labeled_metrics AS (
  SELECT
    {{ attributes }},
    metrics.name AS metric,
    metrics.type AS metric_type,
    value.key AS key,
    value.value AS value
  FROM
    unioned_grouped_labeled_metrics
  CROSS JOIN
    UNNEST(metrics) AS metrics,
    UNNEST(metrics.value) AS value
),
dual_labeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<
      STRUCT<
        name STRING,
        type STRING,
        value ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>
      >
    >[{{ dual_labeled_metrics }}] AS metrics
  FROM
    extracted
  WHERE
    -- If you're changing this, then you'll also need to change probe_counts_v1,
    -- where sampling is taken into account for counting clients.
    channel IN ("nightly", "beta")
    OR (channel = "release" AND os != "Windows")
    OR (channel = "release" AND os = "Windows" AND sample_id < 10)
),
flattened_dual_labeled_metrics AS (
  SELECT
    {{ attributes }},
    metrics.name AS metric,
    metrics.type AS metric_type,
    CONCAT(value.key, '.', nested_value.key) AS key,
    nested_value.value AS value
  FROM
    dual_labeled_metrics
  CROSS JOIN
    UNNEST(metrics) AS metrics,
    UNNEST(metrics.value) AS value,
    UNNEST(value.value) AS nested_value
),
flattened_unioned_labeled_metrics AS (
  SELECT
    *
  FROM
    flattened_labeled_metrics
  UNION ALL
  SELECT
    *
  FROM
    flattened_dual_labeled_metrics
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
    flattened_unioned_labeled_metrics
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
  unioned_unlabeled_metrics
UNION ALL
SELECT
  *
FROM
  labeled_metrics
