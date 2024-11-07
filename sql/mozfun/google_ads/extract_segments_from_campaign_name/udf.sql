CREATE OR REPLACE FUNCTION google_ads.extract_segments_from_campaign_name(campaign_name STRING)
RETURNS STRUCT<campaign_region STRING, campaign_country_code STRING, campaign_language STRING> AS (
    -- Some example campaign names:
    -- Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1
    -- Mozilla_FF_UAC_MGFQ3_KE_EN_CPI
    -- Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7
    -- Mozilla_Firefox_DE_DE_Google_UAC_Android
    -- Mozilla_NA_FF_UAC_Group1_Event1
    -- Mozilla_FF_YouTube_US_EN_Drip_Consideration
  CASE
    -- Monitor Plus
    WHEN REGEXP_CONTAINS(campaign_name, r"^(Mozilla_Monitor_Plus)")
      THEN STRUCT(
          CASE
            WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_")
              THEN "NA"
            ELSE "EU"
          END AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_Monitor_Plus_([A-Z]{2})"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_Monitor_Plus_[A-Z]{2}_([A-Z]{2})"
          ) AS campaign_language
        )
    -- Pocket
    WHEN REGEXP_CONTAINS(campaign_name, r"(Pocket)")
      THEN STRUCT(
          CASE
            WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_")
              THEN "NA"
            ELSE "EU"
          END AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_(?:Pocket_|)|Pocket_Q22023_)([A-Z]{2})"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_(?:Pocket_|)|Pocket_Q22023_)[A-Z]{2}_([A-Z]{2})"
          ) AS campaign_language
        )
    -- Fakespot
    WHEN REGEXP_CONTAINS(campaign_name, r"^Mozilla_Fakespot_")
      THEN STRUCT(
          CASE
            WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA|NA)_")
              THEN "NA"
            ELSE "EU"
          END AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_Fakespot(?:_Display|_ACQ_YouTube|))_([A-Z]{2})_"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_Fakespot(?:_Display|_ACQ_YouTube|))_(?:[A-Z]{2})_([A-Z]{2})_"
          ) AS campaign_language
        )
    -- VPN
    WHEN REGEXP_CONTAINS(campaign_name, r"VPN")
      THEN STRUCT(
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_VPN_(?:Search|UAC_Android|UAC_iOS|Video))_([A-Z]{2})_"
          ) AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_VPN(?:_Search|_UAC_Android|_UAC_iOS|_Video)_[A-Z]{2}_([A-Z]{2})\(Target_States\)_[A-Z]{2}_"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^Mozilla_VPN(?:_Search|_UAC_Android|_UAC_iOS|_Video)_[A-Z]{2}_[A-Z]{2}\(Target_States\)_([A-Z]{2})_"
          ) AS campaign_language
        )
    -- UAC
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
    WHEN REGEXP_CONTAINS(
        campaign_name,
        r"^(Mozilla_FF(?:_Display|_Search|_YouTube|)_[A-Z]{2}_[A-Z]{2}_)"
      )
      THEN STRUCT(
          CASE
            WHEN REGEXP_CONTAINS(campaign_name, r"_(US|CA)_")
              THEN "NA"
            ELSE "EU"
          END AS campaign_region,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_FF(?:_Display|_Search|_YouTube|))_([A-Z]{2})_(?:[A-Z]{2})_"
          ) AS campaign_country_code,
          REGEXP_EXTRACT(
            campaign_name,
            r"^(?:Mozilla_FF(?:_Display|_Search|_YouTube|))_(?:[A-Z]{2})_([A-Z]{2})_"
          ) AS campaign_language
        )
    ELSE NULL
  END
);

SELECT
  -- Test Monitor Plus
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_Monitor_Plus_US_EN_Google_Search_07.05.24-31.07.24_NB"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  -- Test Pocket
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_Pocket_DE_DE_Display"),
    STRUCT("EU" AS campaign_region, "DE" AS campaign_country_code, "DE" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_UK_EN_Pocket _SEM_Q12023_Consideration"
    ),
    STRUCT("EU" AS campaign_region, "UK" AS country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Pocket_Q22023_UK_Google_UAC_Subs_Android"),
    STRUCT("EU" AS campaign_region, "UK" AS country_code, CAST(NULL AS String) AS campaign_language)
  ),
  -- Test Fakespot
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_Fakespot_ACQ_YouTube_US_EN_Awareness"),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_Fakespot_Display_NA_EN_Shoppers"),
    STRUCT("NA" AS campaign_region, "NA" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_Fakespot_US_EN_Search_Awareness_Broad_Desktop"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  -- Test VPN
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_VPN_Search_NA_US(Target_States)_EN_QA_Consideration"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_VPN_UAC_Android_NA_US(Target_States)_EN_QA_Conversion"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_VPN_UAC_iOS_NA_US(Target_States)_EN_QA_Conversion"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_VPN_Video_NA_US(Target_States)_EN_QA_Awareness"
    ),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  -- Test UAC
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    STRUCT("EU" AS campaign_region, "AT" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_MGFQ3_KE_EN_CPI"),
    STRUCT("Expansion" AS campaign_region, "KE" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_UAC_AU_AU_EN_AllGroups_Event7"),
    STRUCT("AU" AS campaign_region, "AU" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_Firefox_DE_DE_Google_UAC_Android"),
    STRUCT("EU" AS campaign_region, "DE" AS campaign_country_code, "DE" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_NA_FF_UAC_Group1_Event1"),
    STRUCT(
      "NA" AS campaign_region,
      CAST(NULL AS STRING) AS campaign_country_code,
      CAST(NULL AS STRING) AS campaign_language
    )
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_CA_EN_UAC_iOS_Installs"),
    STRUCT("NA" AS campaign_region, "CA" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name(
      "Mozilla_FF_DE_DE_UAC_iOSTest3_Installs_POUS2001718"
    ),
    STRUCT("EU" AS campaign_region, "DE" AS campaign_country_code, "DE" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_Display_US_EN_Drip_Awareness"),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_Search_US_EN_Drip_Conversion"),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
  mozfun.assert.struct_equals(
    google_ads.extract_segments_from_campaign_name("Mozilla_FF_YouTube_US_EN_Drip_Consideration"),
    STRUCT("NA" AS campaign_region, "US" AS campaign_country_code, "EN" AS campaign_language)
  ),
