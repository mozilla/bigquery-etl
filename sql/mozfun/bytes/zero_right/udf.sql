
                -- Definition for bytes.zero_right
                -- For more information on writing UDFs see:
                -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION bytes.zero_right()
RETURNS BOOLEAN AS (
  TRUE
);

                -- Tests
SELECT
  assert.true(bytes.zero_right())
