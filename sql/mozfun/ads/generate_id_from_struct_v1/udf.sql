-- Definition for ads.generate_id_from_struct_v1
-- For more information on writing UDFs see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION `moz-fx-dev-lvargas-dev.ads.generate_id_from_struct_v1`(
  key STRUCT<
    adm_date DATE,
    product STRING,
    surface STRING,
    country_code STRING,
    advertiser STRING,
    position INT64
  >
)
RETURNS STRUCT<id INT64, version STRING> AS (
  STRUCT(
    FARM_FINGERPRINT(
      TO_JSON_STRING(
        STRUCT(
          key.adm_date AS adm_date,
          IFNULL(LOWER(TRIM(key.product)), "") AS product,
          IFNULL(LOWER(TRIM(key.surface)), "") AS surface,
          IFNULL(LOWER(TRIM(key.country_code)), "") AS country_code,
          IFNULL(LOWER(TRIM(key.advertiser)), "") AS advertiser,
          key.position AS position
        )
      )
    ) AS id,
    'v1' AS version
  )
);

-- Tests
WITH test_cases AS (
  -- Case 1: Baseline.
  SELECT
    "Base case" AS test_name,
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "Mobile", "AT", "Abcd", 1) AS input,
    3164869416579476752 AS expected_id
  UNION ALL
  -- Case 2: Different values.
  SELECT
    "Change surface",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "M", "AT", "Abcd", 1),
    8870727342259587816
  UNION ALL
  -- Case 3: NULLs.
  SELECT
    "Set pricing model to NULL",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", NULL, "AT", "Abcd", 1),
    -7902997304503704096
  UNION ALL
  -- Case 4: Leading or trailing whitespaces.
  SELECT
    "Multiple white spaces in product.",
    STRUCT(DATE "2026-01-01", "   Sponsored Tile ", "Mobile", "AT", "Abcd", 1),
    3164869416579476752
  UNION ALL
  -- Case 1: Deterministic.
  SELECT
    "Base case again shold return the same id.",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "Mobile", "AT", "Abcd", 1),
    3164869416579476752
  UNION ALL
  -- Case 1: Different Order without field name
  SELECT
    "Base case with fields product and surface in switched order",
    STRUCT(DATE "2026-01-01", "Mobile", "Sponsored Tile", "AT", "Abcd", 1),
    -4679453810365301680
  UNION ALL
  -- Case 1: Different Order with field name
  SELECT
    "Base case with fields product and surface in switched order",
    STRUCT(
      DATE "2026-01-01" AS adm_date,
      "Mobile" AS surface,
      "Sponsored Tile" AS product,
      "AT" AS country_code,
      "Abcd" AS advertiser,
      1 AS position
    ),
    -4679453810365301680
  UNION ALL
  -- Case 6: NULL in all fields.
  SELECT
    "All fields are NULL.",
    STRUCT(
      CAST(NULL AS DATE),
      CAST(NULL AS STRING),
      CAST(NULL AS STRING),
      CAST(NULL AS STRING),
      CAST(NULL AS STRING),
      CAST(NULL AS INT64)
    ),
    506532424075948991
)
SELECT
  `mozfun`.assert.equals((SELECT ads.generate_id_from_struct_v1(input).id), expected_id)
FROM
  test_cases;
