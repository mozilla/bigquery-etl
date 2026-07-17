-- Parses a campaign name into known segments.
CREATE OR REPLACE FUNCTION udf.marketing_parse_campaign_name(campaign_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>)
);
