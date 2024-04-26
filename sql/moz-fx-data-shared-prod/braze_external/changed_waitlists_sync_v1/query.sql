WITH max_update AS (
  SELECT
    MAX(UPDATED_AT) AS max_update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_waitlists_sync_v1`
  LIMIT
    1
)
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
          waitlists_array.create_timestamp AS create_timestamp,
          waitlists_array.update_timestamp AS update_timestamp
        )
        ORDER BY
          waitlists_array.update_timestamp DESC
      ) AS waitlists
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.waitlists_v1` AS waitlists
CROSS JOIN
  UNNEST(waitlists.waitlists) AS waitlists_array,
  max_update
WHERE
  waitlists_array.update_timestamp > (SELECT max_update_timestamp FROM max_update)
GROUP BY
  waitlists.external_id;
