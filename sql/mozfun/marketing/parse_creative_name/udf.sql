CREATE OR REPLACE FUNCTION marketing.parse_creative_name(creative_name STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  CASE
    WHEN REGEXP_CONTAINS(creative_name, r"^gads_v2")
      AND ARRAY_LENGTH(SPLIT(creative_name, "_")) = 7
      THEN ARRAY(
          SELECT AS STRUCT
            keys[off] AS key,
            value
          FROM
            UNNEST(SPLIT(creative_name, "_")) AS value
            WITH OFFSET off,
            (
              SELECT
                [
                  'ad_network',
                  'version',
                  'creative',
                  'format',
                  'dimension',
                  'length',
                  'cta'
                ] AS keys
            )
        )
    WHEN REGEXP_CONTAINS(creative_name, r"^meta_v2")
      AND ARRAY_LENGTH(SPLIT(creative_name, "_")) = 7
      THEN ARRAY(
          SELECT AS STRUCT
            keys[off] AS key,
            value
          FROM
            UNNEST(SPLIT(creative_name, "_")) AS value
            WITH OFFSET off,
            (
              SELECT
                [
                  'ad_network',
                  'version',
                  'creative',
                  'format',
                  'dimension',
                  'length',
                  'cta'
                ] AS keys
            )
        )
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals(
    ARRAY_LENGTH(marketing.parse_creative_name('gads_v2_newDevice_video_mix_30S_download')),
    7
  ),
  assert.equals(
    ARRAY_LENGTH(marketing.parse_creative_name('meta_v2_newDevice_video_mix_30S_download')),
    7
  ),
  assert.null(marketing.parse_creative_name('meta_v1_newDevice_video_mix_30S_download')),
  assert.null(marketing.parse_creative_name('meta_v1_newDevice_video_mix_30S_download_extra')),
  assert.null(marketing.parse_creative_name('')),
  assert.null(marketing.parse_creative_name(CAST(NULL AS STRING))),
