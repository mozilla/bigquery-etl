-- udf_js_flatten
CREATE OR REPLACE FUNCTION glam.histogram_cast_json(
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  '''
    let obj = {};
    histogram.map(function(r) {
        obj[r.key] = parseFloat(r.value.toFixed(4));
    });
    return JSON.stringify(obj);
''';
