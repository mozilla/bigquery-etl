{{ header }}
{% include "scalar_bucket_counts_v1.udf.sql" %}
{% from 'macros.sql' import enumerate_table_combinations %}

{# TODO: remove this import by factoring it out as a proper udf #}
{% include "clients_scalar_aggregates_v1.udf.sql" %}

WITH
{{
    enumerate_table_combinations(
        source_table,
        "all_combos",
        cubed_attributes,
        attribute_combinations
    )
}},
-- Ensure there is a single record per client id
deduplicated_combos AS (
  SELECT
    client_id,
    {{ attributes }},
    udf_merged_user_data(
      ARRAY_CONCAT_AGG(scalar_aggregates)
    ) AS scalar_aggregates
  FROM
    all_combos
  GROUP BY
    client_id,
    {{ attributes }}

),
bucketed_booleans AS (
  SELECT
    client_id,
    {{ attributes }},
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates
  FROM
    deduplicated_combos
),
bucketed_scalars AS (
  SELECT
    client_id,
    {{ attributes }},
    {{ aggregate_attributes }},
    agg_type,
    SAFE_CAST(udf_bucket(SAFE_CAST(value AS FLOAT64)) AS STRING) AS bucket
  FROM
    deduplicated_combos
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    metric_type in ({{ scalar_metric_types }})
),
booleans_and_scalars AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    bucketed_booleans
  CROSS JOIN
    UNNEST(scalar_aggregates)
  UNION ALL
  SELECT
    *
  FROM
    bucketed_scalars
)
SELECT
  {{ attributes }},
  {{ aggregate_attributes }},
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  bucket,
  COUNT(*) AS count
FROM
  booleans_and_scalars
GROUP BY
  {{ attributes }},
  {{ aggregate_attributes }},
  client_agg_type,
  bucket
