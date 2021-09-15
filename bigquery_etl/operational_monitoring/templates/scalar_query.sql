{{ header }}

WITH merged_scalars AS (
    SELECT
        @submission_date AS submission_date,
        client_id,
        SAFE.SUBSTR(application.build_id, 0, 8) AS build_id,
        {% for dimension in dimensions %}
          CAST({{ dimension.sql }} AS STRING) AS {{ dimension.name }},
        {% endfor %}

        -- If a pref is defined, treat it as a rollout with an enabled and disabled branch
        -- otherwise use the branches from the experiment based on the slug
        {% if pref %}
        CASE
          WHEN SAFE_CAST({{pref}} as BOOLEAN) THEN 'enabled'
          WHEN NOT SAFE_CAST({{pref}} as BOOLEAN) THEN 'disabled'
        END
        AS branch,
        {% else %}
        mozfun.map.get_key(
          environment.experiments,
          "{{slug}}"
        ).branch AS branch,
        {% endif %}
        ARRAY<
            STRUCT<
                name STRING,
                agg_type STRING,
                value INT64
            >
        >[
          {% for probe in probes %}
            (
                "{{ probe.name }}",
                "MAX",
                MAX(CAST({{ probe.sql }} AS INT64))
            ),
            (
                "{{ probe.name }}",
                "SUM",
                SUM(CAST({{ probe.sql }} AS INT64))
            )
            {{ "," if not loop.last else "" }}
          {% endfor %}
        ] AS metrics,
    FROM
        `{{source}}`
    WHERE
        DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 60 DAY)
    AND normalized_channel = '{{channel}}'
    GROUP BY
        submission_date,
        client_id,
        build_id,
        {% for dimension in dimensions %}
          {{ dimension.name }},
        {% endfor %}
        branch
),

flattened_scalars AS (
    SELECT *
    FROM merged_scalars
    CROSS JOIN UNNEST(metrics)
    WHERE branch IN (
        -- If branches are not defined, assume it's a rollout
        -- and fall back to branches labeled as enabled/disabled
        {% if branches %}
        {% for branch in branches %}
          "{{ branch }}"
          {{ "," if not loop.last else "" }}
        {% endfor %}
        {% else %}
        "enabled", "disabled"
        {% endif %}
    )
),

log_min_max AS (
  SELECT
    name,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) log_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) log_max
  FROM
    flattened_scalars
  GROUP BY 1),

buckets_by_metric AS (
  SELECT name, ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
    mozfun.glam.histogram_generate_scalar_buckets(log_min, log_max, 100)
  ) AS bucket ORDER BY bucket) AS buckets
  FROM log_min_max)

SELECT
    submission_date,
    client_id,
    build_id,
    cores_count,
    os,
    branch,
    name,
    agg_type,
    -- Replace value with its bucket value
    SAFE_CAST(FORMAT("%.*f", 2, COALESCE(mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)), 0) + 0.0001) AS FLOAT64) AS value
FROM
    flattened_scalars
LEFT JOIN buckets_by_metric USING(name)

