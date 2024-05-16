CREATE OR REPLACE FUNCTION marketing.parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    WHEN REGEXP_CONTAINS(campaign_name, r"^(gads_v2)")
      THEN `moz-fx-data-shared-prod`.marketing_campaign_names.google_ads_v2(campaign_name)
    WHEN REGEXP_CONTAINS(campaign_name, r"^(meta_v2)")
      THEN `moz-fx-data-shared-prod`.marketing_campaign_names.meta_v2(campaign_name)
    WHEN REGEXP_CONTAINS(campaign_name, r"^(asa_v1)")
      THEN `moz-fx-data-shared-prod`.marketing_campaign_names.asa_v1(campaign_name)
    ELSE [
        STRUCT(
          "region" AS key,
          google_ads.extract_segments_from_campaign_name(campaign_name).campaign_region AS value
        ),
        STRUCT(
          "country_code" AS key,
          google_ads.extract_segments_from_campaign_name(campaign_name).campaign_country_code AS value
        ),
        STRUCT(
          "language" AS key,
          google_ads.extract_segments_from_campaign_name(campaign_name).campaign_language AS value
        )
      ]
  END
);

SELECT
  assert.array_equals_any_order(
    marketing.parse_campaign_name("Mozilla_FF_UAC_EU_AT_EN_AllGroups_Event1"),
    [
      STRUCT("region" AS key, "EU" AS value),
      STRUCT("country_code" AS key, "AT" AS value),
      STRUCT("language" AS key, "EN" AS value)
    ]
  ),
  assert.null(marketing.parse_campaign_name(NULL)),
