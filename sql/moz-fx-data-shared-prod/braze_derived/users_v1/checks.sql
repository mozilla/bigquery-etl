-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  WHERE
    users.update_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 HOUR)
) > 0
AS
  "No new records in the braze_derived.users_v1 table in the last 15 hours";

-- macro checks

#fail
{{ not_null(["external_id", "email", "email_subscribe", "has_fxa", "create_timestamp", "update_timestamp"]) }}

#fail
{{ min_row_count(85000000) }}

#fail
{{ is_unique(["external_id", "email", "fxa_id_sha256"]) }}
