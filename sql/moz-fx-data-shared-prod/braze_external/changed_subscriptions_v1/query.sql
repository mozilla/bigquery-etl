    -- CTE to determine the maximum update timestamp from changed_subscriptions_v1
WITH max_update AS (
  SELECT
    MAX(subscriptions.update_timestamp) AS latest_subscription_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS changed,
    UNNEST(changed.subscriptions) AS subscriptions
)
  -- Main query to select all records from subscriptions_v1 that have been updated since the last sync
SELECT
  subscriptions.external_id,
  -- Reconstruct the subscriptions array to include only entries with non-null timestamps greater than max_timestamp
  ARRAY(
    SELECT AS STRUCT
      subscriptions_array.subscription_name AS subscription_name,
      subscriptions_array.firefox_subscription_id AS firefox_subscription_id,
      subscriptions_array.mozilla_subscription_id AS mozilla_subscription_id,
      subscriptions_array.mozilla_dev_subscription_id AS mozilla_dev_subscription_id,
      subscriptions_array.subscription_state AS subscription_state,
      subscriptions_array.update_timestamp AS update_timestamp
    FROM
      UNNEST(subscriptions.subscriptions) AS subscriptions_array
    WHERE
      subscriptions_array.update_timestamp > max_update.latest_subscription_updated_at
      AND subscriptions_array.update_timestamp IS NOT NULL
  ) AS subscriptions
FROM
  `moz-fx-data-shared-prod.braze_derived.subscriptions_v1` AS subscriptions,
  max_update
  -- Filter to include only those rows where the new subscriptions array is not empty
WHERE
  ARRAY_LENGTH(
    ARRAY(
      SELECT
        1
      FROM
        UNNEST(subscriptions.subscriptions) AS subscriptions_array
      WHERE
        subscriptions_array.update_timestamp > max_update.latest_subscription_updated_at
        AND subscriptions_array.update_timestamp IS NOT NULL
    )
  ) > 0;
