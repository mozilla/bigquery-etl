-- Definition for ads.generate_int_id_from_struct
-- For more information on writing UDFs see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION analysis.generate_id_from_struct_v1
(
  key STRUCT<
    adm_date DATE, product STRING, surface STRING, country_code STRING, advertiser STRING, position INT64, pricing_model STRING
  >
) RETURNS STRUCT<id INT64, version STRING> AS (
       STRUCT (
       FARM_FINGERPRINT( TO_JSON_STRING(
          STRUCT(
            key.adm_date AS adm_date,
            IFNULL(LOWER(TRIM(key.product)), "") AS product,
            IFNULL(LOWER(TRIM(key.surface)), "") AS surface,
            IFNULL(LOWER(TRIM(key.country_code)), "") AS country_code,
            IFNULL(LOWER(TRIM(key.advertiser)), "") AS advertiser,
            key.position AS position,
            IFNULL(LOWER(TRIM(key.pricing_model)), "") AS pricing_model
       ) ) ) AS id,
       'v1' AS version
    )
);

-- Tests
WITH test_cases AS (
  -- Case 1: Baseline.
  SELECT
    "Base case" AS test_name,
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "Mobile", "AT", "Allegro", 1, "impressions") AS input,
    -1178731585035914872 AS expected_id
  UNION ALL
  -- Case 2: Different values.
  SELECT
    "Change surface",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "M", "AT", "Allegro", 1, "impressions"),
    5393552329614256041
  UNION ALL
  -- Case 3: NULLs.
  SELECT
    "Set pricing model to NULL",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "Mobile", "AT", "Allegro", 1, NULL),
    -2691566846592782121
  UNION ALL
  -- Case 4: Leading or trailing whitespaces.
  SELECT
    "Multiple white spaces in product.",
    STRUCT(DATE "2026-01-01", "   Sponsored Tile ", "Mobile", "AT", "Allegro", 1, 'impressions'),
    -1178731585035914872
  UNION ALL
  -- Case 1: Deterministic.
  SELECT
    "Base case again shold return the same id.",
    STRUCT(DATE "2026-01-01", "Sponsored Tile", "Mobile", "AT", "Allegro", 1, "impressions"),
    -1178731585035914872
  UNION ALL
  -- Case 1: Different Order without field name
  SELECT
    "Base case with fields product and surface in switched order",
    STRUCT(DATE "2026-01-01", "Mobile", "Sponsored Tile","AT", "Allegro", 1, "impressions"),
    -7757866412077111074
  UNION ALL
  -- Case 1: Different Order with field name
  SELECT
    "Base case with fields product and surface in switched order",
    STRUCT(DATE "2026-01-01" AS adm_date, "Mobile" AS surface, "Sponsored Tile" AS product, "AT" AS country_code, "Allegro" AS advertiser, 1 AS position, "impressions" AS pricing_model),
    -7757866412077111074
  UNION ALL
  -- Case 6: NULL in all fields.
  SELECT
    "All fields are NULL.",
    STRUCT(CAST(NULL AS DATE), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT64), CAST(NULL AS STRING)),
    -604032237644342624 AS expected
)
SELECT
  `mozfun`.assert.equals(
    (
      SELECT analysis.generate_id_from_struct_v1(input).id
    ),
      expected_id
    )
FROM test_cases;
