-- Construct the JSON payload in Braze required format
WITH max_timestamps AS (
  SELECT
    subscriptions.external_id,
    MAX(subscriptions_array.update_timestamp) AS max_update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS subscriptions
  CROSS JOIN
    UNNEST(subscriptions.subscriptions) AS subscriptions_array
  GROUP BY
    subscriptions.external_id
)
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
      max_timestamps.max_update_timestamp AS update_timestamp
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS subscriptions
CROSS JOIN
  UNNEST(subscriptions.subscriptions) AS subscriptions_array
JOIN
  max_timestamps
  ON subscriptions.external_id = max_timestamps.external_id
GROUP BY
  subscriptions.external_id,
  max_timestamps.max_update_timestamp;
