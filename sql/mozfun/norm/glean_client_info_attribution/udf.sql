CREATE OR REPLACE FUNCTION norm.glean_client_info_attribution(
  client_info ANY TYPE,
  attribution_ext JSON,
  distribution_ext JSON
) AS (
  (
    SELECT AS STRUCT
      client_info.* REPLACE (
        (SELECT AS STRUCT client_info.attribution.*, attribution_ext AS ext) AS attribution,
        (SELECT AS STRUCT client_info.distribution.*, distribution_ext AS ext) AS distribution
      )
  )
);

-- Tests
SELECT
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        'abc' AS client_id,
        STRUCT("c" AS campaign, JSON '{"prop": "attr"}' AS ext) AS attribution,
        STRUCT("n" AS name, JSON '{"prop": "dist"}' AS ext) AS distribution
      )
    ),
    TO_JSON_STRING(
      norm.glean_client_info_attribution(
        STRUCT(
          'abc' AS client_id,
          STRUCT("c" AS campaign) AS attribution,
          STRUCT("n" AS name) AS distribution
        ),
        JSON '{"prop": "attr"}',
        JSON '{"prop": "dist"}'
      )
    )
  ),
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        'abc' AS client_id,
        STRUCT("c" AS campaign, CAST(NULL AS JSON) AS ext) AS attribution,
        STRUCT("n" AS name, CAST(NULL AS JSON) AS ext) AS distribution
      )
    ),
    TO_JSON_STRING(
      norm.glean_client_info_attribution(
        STRUCT(
          'abc' AS client_id,
          STRUCT("c" AS campaign) AS attribution,
          STRUCT("n" AS name) AS distribution
        ),
        CAST(NULL AS JSON),
        CAST(NULL AS JSON)
      )
    )
  ),
