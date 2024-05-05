WITH extract_timestamp AS (
  SELECT
    MAX(TO_JSON_STRING(payload.newsletters_v1[0].update_timestamp)) AS max_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_newsletters_sync_v1`
),
max_update AS (
  SELECT
    TIMESTAMP(braze_parse_time(max_updated_at)) AS latest_newsletter_updated_at
  FROM
    extract_timestamp
)
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
