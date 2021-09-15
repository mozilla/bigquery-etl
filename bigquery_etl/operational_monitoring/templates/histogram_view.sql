{{ header }}

CREATE OR REPLACE VIEW
  `{{gcp_project}}.operational_monitoring.{{slug}}_histogram`
AS
WITH normalized AS (
    SELECT
        client_id,
        build_id,
        {% for dimension in dimensions %}
          {{ dimension.name }},
        {% endfor %}
        branch,
        name AS probe,
        STRUCT<
            bucket_count INT64,
            sum INT64,
            histogram_type INT64,
            `range` ARRAY<INT64>,
            VALUES
            ARRAY<STRUCT<key STRING, value FLOAT64>>
        >(
            ANY_VALUE(histogram.bucket_count),
            ANY_VALUE(histogram.sum),
            ANY_VALUE(histogram.histogram_type),
            ANY_VALUE(histogram.range),
            mozfun.glam.histogram_normalized_sum(
                mozfun.hist.merge(ARRAY_AGG(histogram IGNORE NULLS)).values,
                1.0
            )
        ) AS histogram
        FROM `{{gcp_project}}.{{dataset}}.{{slug}}_histogram`
        CROSS JOIN UNNEST(metrics)
        WHERE
            PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            AND DATE(submission_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY
        client_id,
        build_id,
        {% for dimension in dimensions %}
          {{ dimension.name }},
        {% endfor %}
        branch,
        probe)

-- Cast histograms to have FLOAT64 keys
-- so we can use the histogram jackknife percentile function.
SELECT
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch,
    probe,
    STRUCT<
        bucket_count INT64,
        sum INT64,
        histogram_type INT64,
        `range` ARRAY<INT64>,
        VALUES
        ARRAY<STRUCT<key FLOAT64, value FLOAT64>
    >>(histogram.bucket_count,
        histogram.sum,
        histogram.histogram_type,
        histogram.range,
        ARRAY(SELECT AS STRUCT CAST(keyval.key AS FLOAT64), keyval.value FROM UNNEST(histogram.values) keyval)
    ) AS histogram
FROM normalized
