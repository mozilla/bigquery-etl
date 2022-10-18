/*
legacy subscriptions table is documented at:
https://developer.apple.com/documentation/appstorereceipts/responsebody

and the output for this table is documented at
https://developer.apple.com/documentation/appstoreservernotifications/responsebodyv2decodedpayload
*/
WITH legacy_subscriptions AS (
  SELECT
    subscriptions.id,
    users.fxa_uid AS user_id,
    subscriptions.is_active,
    subscriptions.updated_at,
    mozfun.iap.parse_apple_receipt(subscriptions.provider_receipt_json) AS apple_receipt,
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_external.subscriptions_v1 AS subscriptions
  JOIN
    `moz-fx-data-shared-prod`.mozilla_vpn_external.users_v1 AS users
  ON
    (subscriptions.user_id = users.id)
  WHERE
    subscriptions.provider = "APPLE"
)
SELECT
  -- document_id must match legacy subscription_id to avoid backfilling active_subscription_ids
  CAST(id AS STRING) AS legacy_subscription_id,
  updated_at AS event_timestamp,
  CAST(NULL AS STRING) AS auto_renew_product_id,
  CAST(NULL AS STRING) AS auto_renew_status,
  apple_receipt.receipt.bundle_id,
  apple_receipt.environment,
  (
    SELECT
      ANY_VALUE(expiration_intent)
    FROM
      UNNEST(apple_receipt.pending_renewal_info)
  ) AS expiration_intent,
  expires_date,
  CAST(NULL AS STRING) AS form_of_payment,
  CAST(NULL AS TIMESTAMP) AS grace_period_expires_date,
  in_app_ownership_type,
  FALSE AS is_in_billing_retry,
  CAST(NULL AS BOOL) AS is_upgraded,
  CAST(NULL AS STRING) AS latest_notification_subtype,
  CAST(NULL AS STRING) AS latest_notification_type,
  offer_identifier,
  offer_type,
  original_purchase_date,
  original_transaction_id,
  product_id,
  purchase_date,
  revocation_date,
  revocation_reason,
  IF(is_active, 1, 2) AS status,
  "Auto-Renewable Subscription" AS type,
  fxa_uid AS user_id,
  updated_at AS verified_at,
FROM
  legacy_subscriptions
CROSS JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT
        TIMESTAMP_MILLIS(original_purchase_date_ms) AS original_purchase_date,
        TIMESTAMP_MILLIS(expires_date_ms) AS expires_date,
        in_app_ownership_type,
        IF(is_trial_period = "true", 1, NULL) AS offer_type,
        promotional_offer_id AS offer_identifier,
        TIMESTAMP_MILLIS(purchase_date_ms) AS purchase_date,
        TIMESTAMP_MILLIS(cancellation_date_ms) AS revocation_date,
        cancellation_reason AS revocation_reason,
      FROM
        UNNEST(
          ARRAY_CONCAT(
            COALESCE(apple_receipt.latest_receipt_info, []),
            COALESCE(apple_receipt.receipt.in_app, [])
          )
        )
      -- ZetaSQL requires QUALIFY to be used in conjunction with WHERE, GROUP BY, or HAVING.
      WHERE
        TRUE
      QUALIFY
        1 = ROW_NUMBER() OVER (PARTITION BY is_trial_period ORDER BY expires_date_ms DESC)
    )
  )
