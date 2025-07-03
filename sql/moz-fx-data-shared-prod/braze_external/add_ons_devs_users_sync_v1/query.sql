-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  CONCAT(
  'add-ons-devs-',
  SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23)
) AS pocket_email_id

  TO_JSON(
    STRUCT(
      email AS email,
      email_subscribe AS email_subscribe,
      ARRAY_AGG(
        STRUCT(
          subscriptions_array.firefox_subscription_id AS subscription_group_id,
          subscriptions_array.subscription_state AS subscription_state
        )
        )
      ) AS subscription_groups
    )
  AS PAYLOAD
  FROM `moz-fx-data-shared-prod.braze_derived.add_ons_devs_users_sync_v1`