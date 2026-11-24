WITH buckets AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel AS channel,
    distribution.key as label,
    CAST(`values`.key AS INT64) AS key,
    SUM(`values`.value) AS value
  FROM `moz-fx-data-shared-prod.{{ dataset_name }}.metrics`
  CROSS JOIN UNNEST(metrics.{{ metric.table }}.{{ category }}_{{ metric.name }}) as distribution
  CROSS JOIN UNNEST(
    -- Filter out anything where SUM(key * count) is greater than the metric sum
    IF(
      (
        SELECT
          SUM(CAST(bucket.key AS FLOAT64) * bucket.value)
        FROM
          UNNEST(distribution.value.values) AS bucket
      ) <= distribution.value.sum,
      distribution.value.values,
      []
    )
  ) AS `values`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    channel,
    distribution.key,
    key
),
percentiles AS (
  SELECT
    submission_date,
    channel,
    label,
    mozfun.hist.percentiles(
      STRUCT(ARRAY_AGG(STRUCT(key, value)) AS `values`),
      [0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999]
    ) AS p,
    SUM(value) AS sample_count
  FROM
    buckets
  GROUP BY
    submission_date,
    channel,
    label
)
SELECT
    submission_date,
    "{{ application }}" as application,
    channel,
    label,
    p[0].value as q001,
    p[1].value as q01,
    p[2].value as q05,
    p[3].value as q50,
    p[4].value as q95,
    p[5].value as q99,
    p[6].value as q999,
    sample_count
FROM
    percentiles
