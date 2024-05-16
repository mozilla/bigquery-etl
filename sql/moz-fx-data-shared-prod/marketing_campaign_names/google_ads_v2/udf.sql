CREATE OR REPLACE FUNCTION marketing_campaign_names.google_ads_v2(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  IF(
    NOT (
      REGEXP_CONTAINS(campaign_name, r'^gads_v2')
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 18
    ),
    NULL,
    ARRAY(
      SELECT AS STRUCT
        keys[off] AS key,
        value
      FROM
        UNNEST(SPLIT(campaign_name, "_")) AS value
        WITH OFFSET off,
        (
          SELECT
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
            ] AS keys
        )
    )
  )
);

SELECT
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing_campaign_names.google_ads_v2(
        'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
      )
    ),
    18
  ),
  mozfun.assert.null(marketing_campaign_names.google_ads_v2('gads_v2_123')),
  mozfun.assert.null(
    marketing_campaign_names.google_ads_v2(
      'asa_v2_monitorPlus_challengeTheDefault_expansion_pl_all_ypt_pl_mobile_android_appCampaign_conversion_search_tcpa_install_id123_po#123456789'
    )
  ),
