-- raw SQL checks
-- checking to make sure there is at least one UPDATED_AT value

#fail
ASSERT(
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_sync_v1`
  WHERE
    UPDATED_AT IS NOT NULL
) > 0;

-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH max_update AS (
    SELECT
      MAX(UPDATED_AT) AS max_updated_at
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_subscriptions_sync_v1`
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.subscriptions_v1`,
    UNNEST(subscriptions) AS subscriptions,
    max_update
  WHERE
    subscriptions.update_timestamp > max_update.max_updated_at
) > 0;
