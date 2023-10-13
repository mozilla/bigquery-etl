CREATE OR REPLACE FUNCTION assert.histogram_equals(expected ANY TYPE, actual ANY TYPE)
RETURNS BOOLEAN AS (
  CASE
    WHEN expected.bucket_count != actual.bucket_count
      THEN assert.error('bucket count', expected.bucket_count, actual.bucket_count)
    WHEN expected.`sum` != actual.`sum`
      THEN assert.error('sum', expected.`sum`, actual.`sum`)
    WHEN expected.histogram_type != actual.histogram_type
      THEN assert.error('histogram type', expected.histogram_type, actual.histogram_type)
    ELSE assert.array_equals(expected.`range`, actual.`range`)
      AND assert.map_equals(expected.values, actual.values)
  END
);
