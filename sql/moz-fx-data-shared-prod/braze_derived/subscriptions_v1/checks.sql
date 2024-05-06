-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH max_update AS (
    SELECT
      MAX(update_timestamp) AS latest_subscription_updated_at
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1`
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.subscriptions_v1`,
    UNNEST(subscriptions) AS subscriptions,
    max_update
  WHERE
    subscriptions.update_timestamp > max_update.latest_subscription_updated_at
) > 0;

-- macro checks

#fail
{{ not_null(["external_id"]) }} -- to do: add array values

#fail
{{ is_unique(["external_id"]) }}

#fail
{{ min_row_count(1) }}
