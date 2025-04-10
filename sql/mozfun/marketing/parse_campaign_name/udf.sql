CREATE OR REPLACE FUNCTION marketing.parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    -- Campaign Artifact Schema - Google Ads - version 2
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gads_v2)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 18
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'audience',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal',
                  'ad_gap_id',
                  'po'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Google Ads - version 1
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gds_v1)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 15
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Facebook Ads - version 2
    WHEN REGEXP_CONTAINS(campaign_name, r"^(meta_v2)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 17
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type',
                  'optimization_goal',
                  'ad_gap_id',
                  'po'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- Campaign Artifact Schema - Apple Search Ads - version 1
    WHEN REGEXP_CONTAINS(campaign_name, r"^(asa_v1)")
      THEN
        CASE
          WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 14
            THEN mozfun.map.from_lists(
                [
                  'ad_network',
                  'version',
                  'product',
                  'initiative',
                  'region',
                  'country_code',
                  'city',
                  'language',
                  'device',
                  'operating_system',
                  'campaign_type',
                  'campaign_goal',
                  'campaign_group',
                  'bidding_type'
                ],
                SPLIT(campaign_name, "_")
              )
          ELSE NULL
        END
    -- pre Campaign Artifact Schema - Apple Search Ads
    WHEN REGEXP_CONTAINS(campaign_name, r"^Mozilla_Firefox_ASA_")
      THEN [
          STRUCT(
            "region" AS key,
            CASE
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("CA", "US")
                THEN "NA"
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("AT", "BE", "CH", "DE", "ES", "FR", "IT", "NL", "PL", "UK")
                THEN "EU"
              WHEN UPPER(
                  REGEXP_EXTRACT(campaign_name, r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})")
                ) IN ("AU", "JP")
                THEN "Expansion"
              ELSE NULL
            END
          ),
          STRUCT(
            "country_code" AS key,
            REGEXP_EXTRACT(
              campaign_name,
              r"Mozilla_Firefox_ASA_(?:iOSGeoTest_|)([A-Z]{2})"
            ) AS value
          )
        ]
    -- pre Campaign Artifact Schema - Google Ads
    WHEN mozfun.google_ads.extract_segments_from_campaign_name(campaign_name) IS NOT NULL
      THEN [
          STRUCT(
            "region" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_region AS value
          ),
          STRUCT(
            "country_code" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_country_code AS value
          ),
          STRUCT(
            "language" AS key,
            mozfun.google_ads.extract_segments_from_campaign_name(
              campaign_name
            ).campaign_language AS value
          )
        ]
    WHEN ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 16
      THEN [
          STRUCT("ad_network" AS key, SPLIT(campaign_name, "_")[0] AS value),
          STRUCT("version" AS key, SPLIT(campaign_name, "_")[1] AS value),
          STRUCT("product" AS key, SPLIT(campaign_name, "_")[2] AS value),
          STRUCT("initiative" AS key, SPLIT(campaign_name, "_")[3] AS value),
          STRUCT("region" AS key, SPLIT(campaign_name, "_")[4] AS value),
          STRUCT("country_code" AS key, SPLIT(campaign_name, "_")[5] AS value)
        ]
    ELSE NULL
  END
);

SELECT
  -- Test - Campaign Artifact Schema - Google Ads - version 2
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
      )
    ),
    18
  ),
  mozfun.assert.null(marketing.parse_campaign_name('gads_v2_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'asa_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
    )
  ),
  -- Test - Campaign Artifact Schema - Google Ads - version 1
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        "gds_v1_firefox_ctd_EU_DE_all_DE_multiscreen_all_gdn_consideration_brand_cpc_something"
      )
    ),
    15
  ),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      "gds_v1_firefox_ctd_EU_DE_all_DE_multiscreen_all_gdn_consideration_brand_cpc_ctr_something"
    )
  ),
  -- Test - Campaign Artifact Schema - Facebook Ads - version 2
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'meta_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
      )
    ),
    17
  ),
  mozfun.assert.null(marketing.parse_campaign_name('meta_v2_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
    )
  ),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'gads_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
    )
  ),
  -- Test - Campaign Artifact Schema - Apple Search Ads - version 1
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'asa_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
      )
    ),
    14
  ),
  mozfun.assert.null(marketing.parse_campaign_name('asa_v1_123')),
  -- Test - pre Campaign Artifact Schema - Apple Search Ads
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_CA_SearchTab"),
    [STRUCT("region" AS key, "NA" AS value), STRUCT("country_code" AS key, "CA" AS value)]
  ),
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_iOSGeoTest_EN_Test2"),
    [
      STRUCT("region" AS key, CAST(NULL AS string) AS value),
      STRUCT("country_code" AS key, "EN" AS value)
    ]
  ),
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_Firefox_ASA_iOSGeoTest_UK_Test3"),
    [STRUCT("region" AS key, "EU" AS value), STRUCT("country_code" AS key, "UK" AS value)]
  ),
  -- Test - pre Campaign Artifact Schema - Google Ads
  mozfun.assert.map_equals(
    marketing.parse_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    [
      STRUCT("region" AS key, "EU" AS value),
      STRUCT("country_code" AS key, "AT" AS value),
      STRUCT("language" AS key, "EN" AS value)
    ]
  ),
  -- Test - NULL
  mozfun.assert.null(marketing.parse_campaign_name(NULL)),
  -- Test - parse up to country code if there are 15 underscores only
  mozfun.assert.map_equals(
    marketing.parse_campaign_name(
      "gads_v1_firefox_test_na_us_national-test_en_desktop_all_search_conversion_nonbrand_cpc_install_adgap"
    ),
    [
      STRUCT("ad_network" AS key, "gads" AS value),
      STRUCT("version" AS key, "v1" AS value),
      STRUCT("product" AS key, "firefox" AS value),
      STRUCT("initiative" AS key, "test" AS value),
      STRUCT("region" AS key, "na" AS value),
      STRUCT("country_code" AS key, "us" AS value)
    ]
  )
