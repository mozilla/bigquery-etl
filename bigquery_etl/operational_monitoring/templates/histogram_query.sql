{{ header }}

WITH merged_probes AS (
  SELECT
    {% if xaxis == "submission_date" %}
    DATE(submission_timestamp) AS submission_date,
    {% else %}
    @submission_date AS submission_date,
    {% endif %}
    client_id,
    SAFE.SUBSTR(application.build_id, 0, 8) AS build_id,
    {% for dimension in dimensions %}
      CAST({{ dimension.sql }} AS STRING) AS {{ dimension.name }},
    {% endfor %}

    -- If a pref is defined, treat it as a rollout with an enabled and disabled branch.
    -- If branches are provided, use those instead.
    -- If neither a pref or branches are available, use the slug and treat it as a rollout
    -- where those with the slug have the feature enabled and those without do not.
    {% if pref %}
    CASE
      WHEN SAFE_CAST({{pref}} as BOOLEAN) THEN 'enabled'
      WHEN NOT SAFE_CAST({{pref}} as BOOLEAN) THEN 'disabled'
    END
    AS branch,
    {% elif branches %}
    mozfun.map.get_key(
      environment.experiments,
      "{{slug}}"
    ).branch AS branch,
    {% else %}
      CASE WHEN
        mozfun.map.get_key(
          environment.experiments,
          "{{slug}}"
        ).branch IS NULL THEN 'disabled'
      ELSE 'enabled'
      END AS branch,
    {% endif %}
    ARRAY<
      STRUCT<
        metric STRING,
        histograms ARRAY<
          STRUCT<
            bucket_count INT64,
            sum INT64,
            histogram_type INT64,
            `range` ARRAY<INT64>,
            values ARRAY<STRUCT<key INT64, value INT64>>>
        >>
    >[
      {% for probe in probes %}
        (
            "{{ probe.name }}",
            ARRAY_AGG(mozfun.hist.extract({{ probe.sql }}) IGNORE NULLS)
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

merged_histograms AS (
  SELECT
    submission_date,
    client_id,
    build_id,
    branch,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    ARRAY_AGG(
      STRUCT<
        name STRING,
        histogram STRUCT<
          bucket_count INT64,
          sum INT64,
          histogram_type INT64,
          `range` ARRAY<INT64>,
          values ARRAY<STRUCT<key INT64, value INT64>>
        >
      > (
        metric,
        CASE
        WHEN
          histograms IS NULL
        THEN
          NULL
        ELSE
          mozfun.hist.merge(histograms)
        END
      )
    ) AS metrics
  FROM
    merged_probes
  CROSS JOIN
    UNNEST(metrics)
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
  GROUP BY
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch)


-- Cast histograms to have string keys so we can use the histogram normalization function
SELECT
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch,
    ARRAY_AGG(
        STRUCT<
            name STRING,
            histogram STRUCT<
                bucket_count INT64,
                sum INT64,
                histogram_type INT64,
                `range` ARRAY<INT64>,
                VALUES
                ARRAY<STRUCT<key STRING, value INT64>>
            >
        >(name, (histogram.bucket_count,
            histogram.sum,
            histogram.histogram_type,
            histogram.range,
            ARRAY(SELECT AS STRUCT CAST(keyval.key AS STRING), keyval.value FROM UNNEST(histogram.values) keyval))
        )
    ) AS metrics
FROM merged_histograms
CROSS JOIN UNNEST(metrics)
GROUP BY
  submission_date,
  client_id,
  build_id,
  {% for dimension in dimensions %}
    {{ dimension.name }},
  {% endfor %}
  branch
