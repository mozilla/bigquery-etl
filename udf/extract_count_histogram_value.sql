-- Per https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/collection/histograms.html#count
-- count histograms record a single value into the 0 bucket
CREATE TEMP FUNCTION
  udf_extract_count_histogram_value(input STRING) AS (
    udf_get_key(udf_json_extract_histogram(input).values, 0)
  );

-- Tests

WITH
  histogram AS (
    SELECT AS VALUE
      ['{"bucket_count":3,"histogram_type":4,"sum":1,"range":[1,2],"values":{"0":1,"1":0}}',
        '{"range":[1,2],"bucket_count":3,"histogram_type":4,"values":{},"sum":0}']
    ),
  --
  extracted AS (
     SELECT
       udf_extract_count_histogram_value(histogram[OFFSET(0)]) as has_value,
       udf_extract_count_histogram_value(histogram[OFFSET(1)]) as null_value
     FROM
       histogram )
    --
SELECT
  assert_equals(1, has_value),
  assert_null(null_value)
FROM
  extracted
