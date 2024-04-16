CREATE OR REPLACE FUNCTION google_ads.extract_country_code_from_campaign_name(campaign_name STRING)
AS (
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_FF_UAC)") THEN (
      REGEXP_EXTRACT(campaign_name, r"^Mozilla_FF_UAC_(?:[A-Z]{2}|MGFQ3)_([A-Z]{2})")
    )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_Firefox_)") THEN (
      REGEXP_EXTRACT(campaign_name, r"^Mozilla_Firefox_([A-Z]{2})")
    )
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_[A-Z]{2}_FF)") THEN (
      -- These campaigns targeted multiple countries/languages, so we don't know
      NULL
    )
    ELSE NULL
  END
);

SELECT
  assert.equals(google_ads.extract_country_code_from_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"), "AT"),
  assert.equals(google_ads.extract_country_code_from_campaign_name("Mozilla_FF_UAC_MGFQ3_KE_EN_CPI"), "KE"),
  assert.equals(google_ads.extract_country_code_from_campaign_name("Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7"), "AU"),
  assert.equals(google_ads.extract_country_code_from_campaign_name("Mozilla_Firefox_DE_DE_Google_UAC_Android"), "DE"),
  assert.equals(google_ads.extract_country_code_from_campaign_name("Mozilla_NA_FF_UAC_Group1_Event1"), CAST(NULL AS STRING)),
