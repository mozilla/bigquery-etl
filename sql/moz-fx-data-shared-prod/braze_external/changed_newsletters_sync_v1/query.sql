-- Braze requires the timestamps in the JSON payload to be in a certain format
-- that cannot be easily extracted from a JSON type object. We extract the timestamp
-- value and then use a user defined function (UDF) to parse it from the JSON object
WITH extract_timestamp AS (
  SELECT
    TO_JSON_STRING(payload.newsletters_v1[0].update_timestamp) AS extracted_time
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_newsletters_sync_v1`
),
-- Retrieves the maximum newsletter updated timestamp from the last run to only
-- select recently changed records
max_update AS (
  MAX(SELECT
    TIMESTAMP(mozfun.datetime_util.braze_parse_time(extracted_time))) AS latest_newsletter_updated_at
  FROM
    extract_timestamp
)
-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  newsletters.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          newsletters_array.newsletter_name AS newsletter_name,
          newsletters_array.subscribed AS subscribed,
          newsletters_array.newsletter_lang AS newsletter_lang,
          newsletters_array.newsletter_source AS newsletter_source,
          -- braze required format for nested timestamps
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              newsletters_array.create_timestamp,
              'UTC'
            ) AS `$time`
          ) AS created_at,
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              newsletters_array.update_timestamp,
              'UTC'
            ) AS `$time`
          ) AS updated_at
        )
        ORDER BY
          newsletters_array.update_timestamp DESC
      ) AS newsletters_v1
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.newsletters_v1` AS newsletters
CROSS JOIN
  UNNEST(newsletters.newsletters) AS newsletters_array
WHERE
  newsletters_array.update_timestamp > (SELECT latest_newsletter_updated_at FROM max_update)
GROUP BY
  newsletters.external_id;
