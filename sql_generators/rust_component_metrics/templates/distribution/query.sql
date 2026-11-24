WITH buckets AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    normalized_channel AS channel,
    CAST(`values`.key AS INT64) AS key,
    SUM(`values`.value) AS value
  FROM `moz-fx-data-shared-prod.{{ dataset_name }}.metrics`
  CROSS JOIN UNNEST(
    -- Filter out anything where SUM(key * count) is greater than the metric sum
    IF(
      (
        SELECT
          SUM(CAST(bucket.key AS FLOAT64) * bucket.value)
        FROM
          UNNEST(metrics.{{ metric.table }}.{{ category }}_{{ metric.name }}.values) AS bucket
      ) <= metrics.{{ metric.table }}.{{ category }}_{{ metric.name }}.sum,
      metrics.{{ metric.table }}.{{ category }}_{{ metric.name }}.values,
      []
    )
  ) AS `values`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    channel,
    key
),
percentiles AS (
  SELECT
    submission_date,
    channel,
    mozfun.hist.percentiles(
      STRUCT(ARRAY_AGG(STRUCT(key, value)) AS `values`),
      [0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999]
    ) AS p,
    SUM(value) AS sample_count
  FROM
    buckets
  GROUP BY
    submission_date,
    channel
)
SELECT
    submission_date,
    "{{ application }}" as application,
    channel,
    p[0].value AS q001,
    P[1].value AS q01,
    p[2].value AS q05,
    p[3].value AS q50,
    p[4].value AS q95,
    p[5].value AS q99,
    p[6].value AS q999,
    sample_count
FROM
    percentiles
