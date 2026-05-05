CREATE OR REPLACE FUNCTION udf.looker_approx_percentile_distinct_disc(
  a_num ARRAY<STRING>,
  fraction FLOAT64
)
RETURNS FLOAT64 AS (
  (
    SELECT
      AVG(num1)
    FROM
      (
        SELECT
          ROW_NUMBER() OVER (
            ORDER BY
              CAST(REGEXP_EXTRACT(num, '\\|\\|(\\-?\\d+(?:.\\d+)?)$') AS FLOAT64)
          ) - 1 AS rn,
          CAST(REGEXP_EXTRACT(num, '\\|\\|(\\-?\\d+(?:.\\d+)?)$') AS FLOAT64) AS num1,
          COUNT(*) OVER () AS total
        FROM
          UNNEST(a_num) num
        WHERE
          num IS NOT NULL
      )
    WHERE
      rn >= FLOOR(total * fraction - 0.0000001)
      AND rn <= FLOOR(total * fraction)
  )
);
