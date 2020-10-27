-- udf_js_flatten
CREATE OR REPLACE FUNCTION glam.histogram_cast_json(
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  '''
    let obj = {};
    histogram.map(r => {
        obj[r.key] = parseFloat(r.value.toFixed(4));
    });
    return JSON.stringify(obj);
''';

SELECT
  assert.equals(
    '{"0":0.1111,"1":0.6667,"2":0}',
    glam.histogram_cast_json(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 0.111111), ("1", 2.0 / 3), ("2", 0)]
    )
  )
