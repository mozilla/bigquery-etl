
                -- Definition for bytes.extract_bits
                -- For more information on writing UDFs see:
                -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION bytes.extract_bits()
RETURNS BOOLEAN AS (
  TRUE
);

                -- Tests
SELECT
  assert.true(bytes.extract_bits())
