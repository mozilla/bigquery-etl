{{ header }}
WITH
{% if filter_desktop_builds %}
valid_builds AS (
  SELECT DISTINCT
    build.build.id AS build_id
  FROM
    `moz-fx-data-shared-prod.telemetry.buildhub2`
),
{% endif %}
extracted AS (
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
  {% if filter_desktop_builds %}
  INNER JOIN
    valid_builds
  ON
    client_info.app_build = build_id
  {% endif %}
  WHERE
    DATE(submission_timestamp) = {{ submission_date }}
    AND client_info.client_id IS NOT NULL
    {% if filter_desktop_builds %}
    -- some FOG clients on Windows are sending version "1024.0.0"
    -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1768187
    AND client_info.app_display_version != '1024.0.0'
    {% endif %}
),
unlabeled_metrics AS (
  SELECT
    {{ attributes }},
    ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>[
        {{ unlabeled_metrics }}
    ] as scalar_aggregates
  FROM
    extracted
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
    extracted
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
