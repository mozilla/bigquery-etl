CREATE OR REPLACE FUNCTION marketing.parse_ad_group_name(ad_group_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    WHEN REGEXP_CONTAINS(ad_group_name, r"^gads_v2")
      AND ARRAY_LENGTH(SPLIT(ad_group_name, "_")) = 9
      THEN mozfun.map.from_lists(
          [
            'ad_network',
            'version',
            'product',
            'initiative',
            'region',
            'country_code',
            'placement',
            'audience',
            'targeting'
          ],
          SPLIT(ad_group_name, "_")
        )
    WHEN REGEXP_CONTAINS(ad_group_name, r"^meta_v2")
      AND ARRAY_LENGTH(SPLIT(ad_group_name, "_")) = 5
      THEN mozfun.map.from_lists(
          ['ad_network', 'version', 'audience', 'placement', 'targeting'],
          SPLIT(ad_group_name, "_")
        )
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals(
    ARRAY_LENGTH(
      marketing.parse_ad_group_name('gads_v2_monitorPlus_ctd_expansion_us_place_aud_targ')
    ),
    9
  ),
  assert.equals(
    ARRAY_LENGTH(marketing.parse_ad_group_name('meta_v2_principledtrailblazers_all_interest')),
    5
  ),
  assert.null(marketing.parse_ad_group_name('gads_v1_monitorPlus_ctd_expansion_us_place_aud_targ')),
  assert.null(marketing.parse_ad_group_name('gads_v2_ctd_expansion_us_place_aud_targ')),
  assert.null(marketing.parse_ad_group_name('meta_v2_principledtrailblazers_all_interest_extra')),
  assert.null(marketing.parse_ad_group_name('')),
  assert.null(marketing.parse_ad_group_name(CAST(NULL AS STRING))),
