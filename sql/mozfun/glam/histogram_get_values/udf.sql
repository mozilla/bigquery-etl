-- udf_get_values
CREATE OR REPLACE FUNCTION glam.histogram_get_values(required ARRAY<FLOAT64>, VALUES ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  (
    SELECT
      ARRAY_AGG(record)
    FROM
      (
        SELECT
          STRUCT<key STRING, value FLOAT64>(
            CAST(k AS STRING),
            VALUES
              [OFFSET(CAST(k AS INT64))]
          ) AS record
        FROM
          UNNEST(required) AS k
      )
  )
);
