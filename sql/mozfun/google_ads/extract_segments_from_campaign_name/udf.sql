CREATE OR REPLACE FUNCTION google_ads.extract_segments_from_campaign_name(campaign_name STRING)
RETURNS STRUCT<campaign_region STRING, campaign_country_code STRING, campaign_language STRING> AS (
    -- Some example campaign names:
    -- Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1
    -- Mozilla_FF_UAC_MGFQ3_KE_EN_CPI
    -- Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7
    -- Mozilla_Firefox_DE_DE_Google_UAC_Android
    -- Mozilla_NA_FF_UAC_Group1_Event1
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_FF_UAC)")
      THEN STRUCT(
          CASE
            REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
            WHEN "MGFQ3"
              THEN "Expansion"
            ELSE REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
          END AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_FF_UAC_(?:[A-Z]{2}|MGFQ3)_([A-Z]{2})"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_FF_UAC_(?:[A-Z]{2}|MGFQ3)_[A-Z]{2}_([A-Z]{2})"
          ) AS campaign_language
        )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_Firefox_)")
      THEN STRUCT(
          CASE
            WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_")
              THEN "NA"
            ELSE "EU"
          END AS campaign_region,
          REGEXP_EXTRACT(campaign_name, r"^Mozilla_Firefox_([A-Z]{2})") AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_Firefox_[A-Z]{2}_([A-Z]{2})"
          ) AS campaign_language
        )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_[A-Z]{2}_FF)")
      THEN
        -- These campaigns targeted multiple countries/languages, so we don't know
        STRUCT(
          REGEXP_EXTRACT(campaign_name, r'Mozilla_([A-Z]{2})_FF') AS campaign_region,
          CAST(NULL AS STRING) AS campaign_country_code,
          CAST(NULL AS STRING) AS campaign_language
        )
    ELSE NULL
  END
);

SELECT
  assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    STRUCT("EU" AS campaign_region, "AT" AS campaign_country_code, "EN" AS campaign_language)
  ),
  assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_MGFQ3_KE_EN_CPI"),
    STRUCT("Expansion" AS campaign_region, "KE" AS campaign_country_code, "EN" AS campaign_language)
  ),
  assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7"),
    STRUCT("AU" AS campaign_region, "AU" AS campaign_country_code, "EN" AS campaign_language)
  ),
  assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_Firefox_DE_DE_Google_UAC_Android"),
    STRUCT("EU" AS campaign_region, "DE" AS campaign_country_code, "DE" AS campaign_language)
  ),
  assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_NA_FF_UAC_Group1_Event1"),
    STRUCT(
      "NA" AS campaign_region,
      CAST(NULL AS STRING) AS campaign_country_code,
      CAST(NULL AS STRING) AS campaign_language
    )
  ),
