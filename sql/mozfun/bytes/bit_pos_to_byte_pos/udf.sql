
                -- Definition for bytes.bit_pos_to_byte_pos
                -- For more information on writing UDFs see:
                -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION bytes.bit_pos_to_byte_pos()
RETURNS BOOLEAN AS (
  TRUE
);

                -- Tests
SELECT
  assert.true(bytes.bit_pos_to_byte_pos())
