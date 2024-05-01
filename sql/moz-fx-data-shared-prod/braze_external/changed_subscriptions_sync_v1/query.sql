WITH max_update AS (
  SELECT
    MAX(UPDATED_AT) AS max_update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_sync_v1`
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  subscriptions.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          subscriptions_array.subscription_name AS subscription_name,
          subscriptions_array.firefox_subscription_id AS firefox_subscription_id,
          subscriptions_array.mozilla_subscription_id AS mozilla_subscription_id,
          subscriptions_array.mozilla_dev_subscription_id AS mozilla_dev_subscription_id,
          subscriptions_array.subscription_state AS subscription_state,
          subscriptions_array.update_timestamp AS update_timestamp
        )
        ORDER BY
          subscriptions_array.update_timestamp DESC
      ) AS subscriptions
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.subscriptions_v1` AS subscriptions
CROSS JOIN
  UNNEST(subscriptions.subscriptions) AS subscriptions_array
WHERE
  subscriptions_array.update_timestamp > (SELECT max_update_timestamp FROM max_update)
GROUP BY
  subscriptions.external_id;
