/*
Casts an ARRAY<STRUCT<key, value>> histogram to a JSON string.
This implementation uses String concatenation instead of
BigQuery native JSON (TO_JSON / JSON_OBJECT) functions to
preserve order.
https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object
Order is important for GLAM histograms so other UDFs that
operate on them, such as glam.percentile, can work correctly.
*/
CREATE OR REPLACE FUNCTION glam.histogram_cast_json(
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS STRING AS (
  (
    SELECT
      COALESCE(
        CONCAT(
          '{',
          STRING_AGG(
            CONCAT('"', key, '":', ROUND(value, 4))
            ORDER BY
              COALESCE(SAFE_CAST(key AS FLOAT64)),
              key
          ),
          '}'
        ),
        "{}"
      )
    FROM
      UNNEST(histogram)
  )
);

SELECT
  assert.equals(
    '{"0":0.1111,"1":0.6667,"2":0}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 0.111111), ("1", 2.0 / 3), ("2", 0)]
    )
  ),
  assert.equals(
    '{"0":0.1111,"1":0.6667,"2":0,"10":100}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[
        ("0", 0.111111),
        ("1", 2.0 / 3),
        ("10", 100),
        ("2", 0)
      ]
    )
  ),
  assert.equals('{}', glam.histogram_cast_json(ARRAY<STRUCT<key STRING, value FLOAT64>>[])),
  assert.equals(
    '{"always":0.5,"never":0.5}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("always", 0.5), ("never", 0.5)]
    )
  ),
