{{ header }}

WITH merged_probes AS (
  SELECT
    @submission_date AS submission_date,
    client_id,
    SAFE.SUBSTR(application.build_id, 0, 8) AS build_id,
    {% for dimension in dimensions %}
      CAST({{ dimension.sql }} AS STRING) AS {{ dimension.name }},
    {% endfor %}
    mozfun.map.get_key(
      environment.experiments,
      "{{slug}}"
    ).branch AS branch,
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
    DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 30 DAY)
  GROUP BY
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch
)

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
      histograms STRUCT<
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
  {% for branch in branches %}
    "{{ branch }}"
    {{ "," if not loop.last else "" }}
  {% endfor %}
)
GROUP BY
  submission_date,
  client_id,
  build_id,
  {% for dimension in dimensions %}
    {{ dimension.name }},
  {% endfor %}
  branch
