-- get the latest update timestamp from the last sync
WITH max_update AS (
  SELECT
    MAX(
      CAST(JSON_EXTRACT_SCALAR(payload, '$.update_timestamp') AS TIMESTAMP)
    ) AS latest_subscription_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_firefox_subscriptions_sync_v1`
),
-- get the max update timestamp for each external_id in subscriptions_v1
max_subscriptions AS (
  SELECT
    external_id,
    MAX(subscriptions.update_timestamp) AS update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_derived.subscriptions_v1`
  CROSS JOIN
    UNNEST(subscriptions) AS subscriptions
  GROUP BY
    external_id
)
-- select all records from subscriptions_v1 that have been updated since the last sync
-- and construct JSON payload for Braze sync
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  subscriptions.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          subscriptions_array.firefox_subscription_id AS subscription_group_id,
          subscriptions_array.subscription_state AS subscription_state
        )
        ORDER BY
          subscriptions_array.update_timestamp DESC
      ) AS subscription_groups,
      max_subscriptions.update_timestamp
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.subscriptions_v1` AS subscriptions
CROSS JOIN
  UNNEST(subscriptions.subscriptions) AS subscriptions_array
JOIN
  max_subscriptions
  ON subscriptions.external_id = max_subscriptions.external_id
WHERE
  subscriptions_array.update_timestamp > (SELECT latest_subscription_updated_at FROM max_update)
GROUP BY
  subscriptions.external_id,
  max_subscriptions.update_timestamp
