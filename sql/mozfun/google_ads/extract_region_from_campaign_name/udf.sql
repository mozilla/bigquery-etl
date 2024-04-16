CREATE OR REPLACE FUNCTION google_ads.extract_region_from_campaign_name(campaign_name STRING)
AS (
    -- Some example campaign names:
    -- Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1
    -- Mozilla_FF_UAC_MGFQ3_KE_EN_CPI
    -- Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7
    -- Mozilla_Firefox_DE_DE_Google_UAC_Android
    -- Mozilla_NA_FF_UAC_Group1_Event1
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_FF_UAC)") THEN (
      CASE REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
        WHEN "MGFQ3" THEN "Expansion"
        ELSE REGEXP_EXTRACT(campaign_name, r"Mozilla_FF_UAC_([A-Z1-9]+)_")
      END
    )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_Firefox_)") THEN (
      CASE
        WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_") THEN "NA"
        ELSE "EU"
      END
    )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_[A-Z]{2}_FF)") THEN (
      REGEXP_EXTRACT(campaign_name, r'Mozilla_([A-Z]{2})_FF')
    )
    ELSE NULL
  END
);

SELECT
  assert.equals(google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"), "EU"),
  assert.equals(google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_MGFQ3_KE_EN_CPI"), "Expansion"),
  assert.equals(google_ads.extract_region_from_campaign_name("Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7"), "AU"),
  assert.equals(google_ads.extract_region_from_campaign_name("Mozilla_Firefox_DE_DE_Google_UAC_Android"), "EU"),
  assert.equals(google_ads.extract_region_from_campaign_name("Mozilla_NA_FF_UAC_Group1_Event1"), "NA"),
