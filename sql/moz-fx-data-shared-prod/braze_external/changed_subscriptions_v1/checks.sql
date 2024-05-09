-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.subscriptions_v1`,
    UNNEST(subscriptions) AS subscriptions
  WHERE
    subscriptions.update_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 HOUR)
) > 0
AS
  "No new records in the braze_external.changed_subscriptions_v1 table in the last 15 hours";

-- macro checks

#warn
{{ not_null(["external_id"]) }}

#warn
{{ min_row_count(1) }}

#warn
{{ is_unique(["external_id"]) }}
