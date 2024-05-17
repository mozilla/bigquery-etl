CREATE OR REPLACE FUNCTION marketing_campaign_names.asa_v1(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  IF(
    NOT (
      REGEXP_CONTAINS(campaign_name, r'^asa_v1')
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 14
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
              'language',
              'device',
              'operating_system',
              'campaign_type',
              'campaign_goal',
              'campaign_group',
              'bidding_type'
            ] AS keys
        )
    )
  )
);

SELECT
  mozfun.assert.equals(
    ARRAY_LENGTH(
      marketing_campaign_names.asa_v1(
        'asa_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
      )
    ),
    14
  ),
  mozfun.assert.null(marketing_campaign_names.asa_v1('asa_v1_123')),
  mozfun.assert.null(
    marketing_campaign_names.asa_v1(
      'gads_v1_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_search_conversion_brand_cpi'
    )
  ),
