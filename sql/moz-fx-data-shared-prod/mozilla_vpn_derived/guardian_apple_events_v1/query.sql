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
  -- WARNING: mozdata.subscription_platform.apple_subscriptions and
  -- mozdata.subscription_platform.nonprod_apple_subscriptions require field order of
  -- moz-fx-data-shared-prod.mozilla_vpn_derived.guardian_apple_events_v1 to exactly match:
  --   legacy_subscription_id,
  --   event_timestamp,
  --   mozfun.iap.parse_apple_event(`data`).*,
  CAST(legacy_subscriptions.id AS STRING) AS legacy_subscription_id,
  legacy_subscriptions.updated_at AS event_timestamp,
  renewal_info.auto_renew_product_id,
  renewal_info.auto_renew_status,
  legacy_subscriptions.apple_receipt.receipt.bundle_id,
  legacy_subscriptions.apple_receipt.environment,
  renewal_info.expiration_intent,
  receipt_info.expires_date,
  CAST(NULL AS STRING) AS form_of_payment,
  CAST(NULL AS TIMESTAMP) AS grace_period_expires_date,
  receipt_info.in_app_ownership_type,
  renewal_info.is_in_billing_retry_period = 1 AS is_in_billing_retry,
  CAST(NULL AS BOOL) AS is_upgraded,
  CAST(NULL AS STRING) AS latest_notification_subtype,
  CAST(NULL AS STRING) AS latest_notification_type,
  receipt_info.offer_identifier,
  receipt_info.offer_type,
  receipt_info.original_purchase_date,
  receipt_info.original_transaction_id,
  receipt_info.product_id,
  receipt_info.purchase_date,
  receipt_info.revocation_date,
  receipt_info.revocation_reason,
  CASE -- https://developer.apple.com/documentation/appstoreserverapi/status
  WHEN
    legacy_subscriptions.apple_receipt.receipt.request_date < receipt_info.expires_date
  THEN
    1
  WHEN
    renewal_info.is_in_billing_retry
  THEN
    3
  ELSE
    2
  END
  AS status,
  "Auto-Renewable Subscription" AS type,
  legacy_subscriptions.user_id,
  legacy_subscriptions.apple_receipt.receipt.request_date AS verified_at,
FROM
  legacy_subscriptions
CROSS JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT
        TIMESTAMP_MILLIS(expires_date_ms) AS expires_date,
        in_app_ownership_type,
        promotional_offer_id AS offer_identifier,
        IF(is_trial_period = "true", 1, NULL) AS offer_type,
        TIMESTAMP_MILLIS(original_purchase_date_ms) AS original_purchase_date,
        original_transaction_id,
        product_id,
        TIMESTAMP_MILLIS(purchase_date_ms) AS purchase_date,
        TIMESTAMP_MILLIS(cancellation_date_ms) AS revocation_date,
        cancellation_reason AS revocation_reason,
      FROM
        UNNEST(
          ARRAY_CONCAT(
            COALESCE(legacy_subscriptions.apple_receipt.latest_receipt_info, []),
            COALESCE(legacy_subscriptions.apple_receipt.receipt.in_app, [])
          )
        )
      -- ZetaSQL requires QUALIFY to be used in conjunction with WHERE, GROUP BY, or HAVING.
      WHERE
        TRUE
      QUALIFY
        1 = ROW_NUMBER() OVER (PARTITION BY is_trial_period ORDER BY expires_date_ms DESC)
    )
  ) AS receipt_info
LEFT JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT
        renewal_info.auto_renew_product_id,
        renewal_info.auto_renew_status,
        renewal_info.expiration_intent,
        renewal_info.is_in_billing_retry_period = 1 AS is_in_billing_retry,
      FROM
        UNNEST(apple_receipt.pending_renewal_info) AS renewal_info
      WHERE
        receipt_info.original_transaction_id = renewal_info.original_transaction_id
      LIMIT
        1
    )
  ) AS renewal_info
