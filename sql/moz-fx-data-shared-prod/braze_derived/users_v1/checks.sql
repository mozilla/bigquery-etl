-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH extract_timestamp AS (
    SELECT
      TO_JSON_STRING(payload.users_v1[0].update_timestamp) AS extracted_time
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_users_sync_v1`
  ),
-- Retrieves the maximum users updated timestamp from the last run to only
-- select recently changed records
  max_update AS (
    MAX(
      SELECT
        TIMESTAMP(mozfun.datetime_util.braze_parse_time(extracted_time))
    ) AS latest_user_updated_at
    FROM
      extract_timestamp
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.users_v1` AS users,
    max_update
  WHERE
    users.update_timestamp > max_update.latest_user_updated_at
) > 0;

-- macro checks

#fail
{{ not_null(["external_id", "email", "email_subscribe", "has_fxa", "create_timestamp", "update_timestamp"]) }}

#fail
{{ min_row_count(85000000) }}

#fail
{{ is_unique(["external_id", "email", "fxa_id_sha256"]) }}
