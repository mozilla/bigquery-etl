CREATE OR REPLACE FUNCTION marketing.parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  `moz-fx-data-shared-prod`.udf.marketing_parse_campaign_name(campaign_name)
);

-- Tests
SELECT
  mozfun.assert.null(marketing.parse_campaign_name(NULL));
