CREATE OR REPLACE FUNCTION marketing.parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gads_v2)")
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 18
      THEN mozfun.map.from_lists(
          [
            'ad_network',
            'version',
            'product',
            'iniative',
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
    WHEN REGEXP_CONTAINS(campaign_name, r"^(meta_v2)")
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 17
      THEN mozfun.map.from_lists(
          [
            'ad_network',
            'version',
            'product',
            'iniative',
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
    WHEN REGEXP_CONTAINS(campaign_name, r"^(asa_v1)")
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 14
      THEN mozfun.map.from_lists(
          [
            'ad_network',
            'version',
            'product',
            'iniative',
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
    ELSE NULL
  END
);

SELECT
  assert.map_equals(
    marketing.parse_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    [
      STRUCT("region" AS key, "EU" AS value),
      STRUCT("country_code" AS key, "AT" AS value),
      STRUCT("language" AS key, "EN" AS value)
    ]
  ),
  assert.null(marketing.parse_campaign_name(NULL)),
  -- Google Ads tests
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
  -- Meta tests
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
  -- ASA tests
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing.parse_campaign_name(
        'asa_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
      )
    ),
    14
  ),
  mozfun.assert.null(marketing.parse_campaign_name('asa_v1_123')),
  mozfun.assert.null(
    marketing.parse_campaign_name(
      'gads_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
    )
  ),
