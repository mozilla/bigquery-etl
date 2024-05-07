WITH max_update_timestamp AS (
  SELECT
    MAX(subscriptions.update_timestamp) AS latest_subscription_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS changed,
    UNNEST(changed.subscriptions) AS subscriptions
),
filtered_subscriptions AS (
  SELECT
    subscriptions.external_id,
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
        subscriptions_array.update_timestamp > max_update_timestamp.latest_subscription_updated_at
        AND subscriptions_array.update_timestamp IS NOT NULL
    ) AS subscriptions
  FROM
    `moz-fx-data-shared-prod.braze_derived.subscriptions_v1` AS subscriptions
  CROSS JOIN
    max_update_timestamp
)
SELECT
  external_id,
  subscriptions
FROM
  filtered_subscriptions
WHERE
  ARRAY_LENGTH(subscriptions) > 0;
