CREATE OR REPLACE FUNCTION norm.partnership(distribution_id STRING)
RETURNS STRING AS (
  CASE
    WHEN STARTS_WITH(LOWER(distribution_id), "vivo-")
      THEN "vivo"
    WHEN STARTS_WITH(LOWER(distribution_id), "dt-")
      THEN "dt"
    ELSE CAST(NULL AS STRING)
  END
);

-- Tests
SELECT
  assert.equals("vivo", norm.partnership("vivo-123")),
  assert.equals("dt", norm.partnership("DT-123")),
  assert.equals("vivo", norm.partnership("VIVO-123"));
