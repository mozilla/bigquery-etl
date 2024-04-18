CREATE OR REPLACE FUNCTION google_ads.extract_region_from_campaign_name(campaign_name STRING)
RETURNS STRUCT(region STRING, country_code STRING, language STRING)
AS (
    -- Some example campaign names:
    -- Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1
    -- Mozilla_FF_UAC_MGFQ3_KE_EN_CPI
    -- Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7
    -- Mozilla_Firefox_DE_DE_Google_UAC_Android
    -- Mozilla_NA_FF_UAC_Group1_Event1
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_FF_UAC)")
      THEN
        STRUCT(
          CASE
            REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
            WHEN "MGFQ3"
              THEN "Expansion"
            ELSE REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
          END AS region,
          REGEXP_EXTRACT(campaign_name, r"^Mozilla_FF_UAC_(?:[A-Z]{2}|MGFQ3)_([A-Z]{2})") AS country_code,
          REGEXP_EXTRACT(campaign_name, r"^Mozilla_FF_UAC_(?:[A-Z]{2}|MGFQ3)_[A-Z]{2}_([A-Z]{2})") AS language
        )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_Firefox_)")
      THEN
        STRUCT(
          CASE WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_") THEN "NA" ELSE "EU" END AS region,
          REGEXP_EXTRACT(campaign_name, r"^Mozilla_Firefox_([A-Z]{2})") AS country_code,
          REGEXP_EXTRACT(campaign_name, r"^Mozilla_Firefox_[A-Z]{2}_([A-Z]{2})") AS language
        )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_[A-Z]{2}_FF)")
      THEN
        -- These campaigns targeted multiple countries/languages, so we don't know
        STRUCT(
          REGEXP_EXTRACT(campaign_name, r'Mozilla_([A-Z]{2})_FF') AS region,
          CAST(NULL AS STRING) AS country_code,
          CAST(NULL AS STRING) AS language
        )
    ELSE NULL
  END
);

SELECT
  assert.struct_equals(
    google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    STRUCT("EU" AS region, "AT" AS country_code, "EN" AS language)
  ),
  assert.equals(
    google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_MGFQ3_KE_EN_CPI"),
    STRUCT("Expansion" AS region, "KE" AS country_code, "EN" AS language)
  ),
  assert.equals(
    google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7"),
    STRUCT("AU" AS region, "AU" AS country_code, "EN" AS language)
  ),
  assert.equals(
    google_ads.extract_region_from_campaign_name("Mozilla_Firefox_DE_DE_Google_UAC_Android"),
    STRUCT("EU" AS region, "DE" AS country_code, "DE" AS language)
  ),
  assert.equals(
    google_ads.extract_region_from_campaign_name("Mozilla_NA_FF_UAC_Group1_Event1"),
    STRUCT("NA" AS region, CAST(NULL AS STRING) AS country_code, CAST(NULL AS STRING) AS language)
  ),
