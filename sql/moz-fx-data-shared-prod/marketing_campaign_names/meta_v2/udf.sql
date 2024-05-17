CREATE OR REPLACE FUNCTION marketing_campaign_names.meta_v2(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  IF(
    NOT (
      REGEXP_CONTAINS(campaign_name, r'^meta_v2')
      AND ARRAY_LENGTH(SPLIT(campaign_name, "_")) = 17
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
      marketing_campaign_names.meta_v2(
        'meta_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
      )
    ),
    17
  ),
  mozfun.assert.null(marketing_campaign_names.meta_v2('meta_v2_123')),
  mozfun.assert.null(
    marketing_campaign_names.meta_v2(
      'gads_v2_monitorPlus_challengeTheDefault_expansion_pl_all_pl_mobile_android_poll_consideration_search_cpi_install_id123_po#123456789'
    )
  ),
