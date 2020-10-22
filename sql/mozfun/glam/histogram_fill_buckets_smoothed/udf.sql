-- udf_fill_buckets
CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_smoothed(
  input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
  buckets ARRAY<STRING>,
  total_users INT64
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        -- Dirichlet distribution density for each bucket in a histogram
        -- https://docs.google.com/document/d/1ipy1oFIKDvHr3R6Ku0goRjS11R1ZH1z2gygOGkSdqUg
        SAFE_DIVIDE(
          COALESCE(e.value, 0.0) + SAFE_DIVIDE(1, ARRAY_LENGTH(buckets)),
          total_users + 1
        ) AS value
      FROM
        UNNEST(buckets) AS key
      LEFT JOIN
        UNNEST(input_map) AS e
      ON
        SAFE_CAST(key AS STRING) = e.key
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(SAFE_CAST(key AS STRING), value))
    FROM
      total_counts
  )
);
