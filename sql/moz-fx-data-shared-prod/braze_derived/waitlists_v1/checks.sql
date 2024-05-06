-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH max_update AS (
    SELECT
      MAX(
        TIMESTAMP(JSON_VALUE(payload.newsletters_v1[0].update_timestamp, '$."$time"'))
      ) AS latest_waitlist_updated_at
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_waitlists_sync_v1`
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.waitlists_v1`,
    UNNEST(waitlists) AS waitlists,
    max_update
  WHERE
    waitlists.update_timestamp > max_update.latest_waitlist_updated_at
) > 0;

#fail
{{ not_null(["external_id"]) }} -- to do: add array values

#fail
{{ is_unique(["external_id"]) }}

#fail
{{ min_row_count(1) }}
