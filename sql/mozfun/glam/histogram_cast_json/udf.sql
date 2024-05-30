-- udf_js_flatten
CREATE OR REPLACE FUNCTION glam.histogram_cast_json(
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS STRING
AS (
  IF(
    ARRAY_LENGTH(histogram) = 0,
    "{}",
    TO_JSON_STRING(
      JSON_OBJECT(
        ARRAY(SELECT key FROM UNNEST(histogram)),
        ARRAY(SELECT ROUND(value, 4) FROM UNNEST(histogram))
      )
    )
  )
);

SELECT
  assert.equals(
    '{"0":0.1111,"1":0.6667,"2":0.0}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 0.111111), ("1", 2.0 / 3), ("2", 0)]
    )
  ),
  assert.equals(
    '{}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[]
    )
  ),
