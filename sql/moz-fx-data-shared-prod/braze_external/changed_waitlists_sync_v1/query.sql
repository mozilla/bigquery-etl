  -- Retrieves the maximum newsletter updated timestamp from the last run to only
  -- select recently changed records
WITH max_update AS (
  SELECT
    MAX(
      TIMESTAMP(JSON_VALUE(payload.waitlists_v1[0].updated_at, '$."$time"'))
    ) AS latest_waitlist_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_waitlists_sync_v1`
)
  -- Construct the JSON payload in Braze required format
-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  waitlists.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          waitlists_array.waitlist_name AS waitlist_name,
          waitlists_array.waitlist_geo AS waitlist_geo,
          waitlists_array.waitlist_platform AS waitlist_platform,
          waitlists_array.waitlist_source AS waitlist_source,
          waitlists_array.subscribed AS subscribed,
          -- braze required format for nested timestamps
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              waitlists_array.create_timestamp,
              'UTC'
            ) AS `$time`
          ) AS created_at,
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              waitlists_array.update_timestamp,
              'UTC'
            ) AS `$time`
          ) AS updated_at
        )
        ORDER BY
          waitlists_array.update_timestamp DESC
      ) AS waitlists_v1
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.waitlists_v1` AS waitlists
CROSS JOIN
  UNNEST(waitlists.waitlists) AS waitlists_array
WHERE
  waitlists_array.update_timestamp > (SELECT latest_waitlist_updated_at FROM max_update)
GROUP BY
  waitlists.external_id;
