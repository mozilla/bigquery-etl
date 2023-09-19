/*
legacy subscriptions table is documented at:
https://developer.apple.com/documentation/appstorereceipts/responsebody

and the output for this table is documented at
https://developer.apple.com/documentation/appstoreservernotifications/responsebodyv2decodedpayload
*/
WITH legacy_subscriptions AS (
  SELECT
    users.fxa_uid AS user_id,
    subscriptions.is_active,
    subscriptions.updated_at,
    mozfun.iap.parse_apple_receipt(subscriptions.provider_receipt_json) AS apple_receipt,
  FROM
    -- Use a snapshot of the subscriptions table from 2023-01-17 because the Apple receipts
    -- were getting erased from the subscriptions migrated to FxA in December 2022 (VPN-3921),
    -- and the provider-related columns will be removed from the subscriptions table (VPN-3797).
    `moz-fx-data-shared-prod`.mozilla_vpn_external.subscriptions_20230117 AS subscriptions
  JOIN
    `moz-fx-data-shared-prod`.mozilla_vpn_external.users_v1 AS users
  ON
    (subscriptions.user_id = users.id)
  WHERE
    -- Subscriptions migrated to fxa no longer have subscriptions.provider = "APPLE"
    subscriptions.provider_receipt_json IS NOT NULL
    AND JSON_VALUE(provider_receipt_json, "$.receipt.bundle_id") = "org.mozilla.ios.FirefoxVPN"
    -- Exclude duplicate subscriptions rejected by FxA migration
    AND subscriptions.provider IS DISTINCT FROM "FXANOMIGRATE"
)
SELECT
  -- WARNING: subscription_platform_derived.apple_subscriptions_v1 and
  -- subscription_platform_derived.nonprod_apple_subscriptions_v1 require field order of
  -- mozilla_vpn_derived.guardian_apple_events_v1 to exactly match:
  --   event_timestamp,
  --   mozfun.iap.parse_apple_event(`data`).*,
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
  renewal_info.is_in_billing_retry,
  CAST(NULL AS BOOL) AS is_upgraded,
  CAST(NULL AS STRING) AS latest_notification_subtype,
  CAST(NULL AS STRING) AS latest_notification_type,
  receipt_info.offer_identifier,
  receipt_info.offer_type,
  receipt_info.original_purchase_date,
  latest_original_transaction_id AS original_transaction_id,
  receipt_info.product_id,
  receipt_info.purchase_date,
  receipt_info.revocation_date,
  receipt_info.revocation_reason,
  CASE -- https://developer.apple.com/documentation/appstoreserverapi/status
    WHEN receipt_info.source = "receipt.in_app"
      THEN 1 -- subscription was active for the values in this receipt
    WHEN TIMESTAMP_MILLIS(
        legacy_subscriptions.apple_receipt.receipt.request_date_ms
      ) < receipt_info.expires_date
      THEN 1
    WHEN renewal_info.is_in_billing_retry
      THEN 3
    ELSE 2
  END AS status,
  "Auto-Renewable Subscription" AS type,
  legacy_subscriptions.user_id,
  IF(
    receipt_info.source = "receipt.in_app",
    receipt_info.purchase_date,
    TIMESTAMP_MILLIS(legacy_subscriptions.apple_receipt.receipt.request_date_ms)
  ) AS verified_at,
FROM
  legacy_subscriptions
CROSS JOIN
  UNNEST(
    ARRAY(
      SELECT
        original_transaction_id
      FROM
        UNNEST(legacy_subscriptions.apple_receipt.latest_receipt_info)
      ORDER BY
        original_purchase_date_ms DESC
      LIMIT
        1
    )
  ) AS latest_original_transaction_id
CROSS JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT
        source,
        TIMESTAMP_MILLIS(expires_date_ms) AS expires_date,
        in_app_ownership_type,
        promotional_offer_id AS offer_identifier,
        IF(is_trial_period = "true", 1, NULL) AS offer_type,
        TIMESTAMP_MILLIS(original_purchase_date_ms) AS original_purchase_date,
        product_id,
        TIMESTAMP_MILLIS(purchase_date_ms) AS purchase_date,
        TIMESTAMP_MILLIS(cancellation_date_ms) AS revocation_date,
        CAST(cancellation_reason AS INT64) AS revocation_reason,
      FROM
        UNNEST(
          [
            -- in order by priority
            STRUCT(
              "latest_receipt_info" AS source,
              legacy_subscriptions.apple_receipt.latest_receipt_info AS receipts
            ),
            STRUCT(
              "receipt.in_app" AS source,
              legacy_subscriptions.apple_receipt.receipt.in_app AS receipts
            )
          ]
        )
        WITH OFFSET AS _source_offset
      CROSS JOIN
        UNNEST(COALESCE(receipts, []))
        WITH OFFSET AS _receipts_offset
      WHERE
        latest_original_transaction_id = original_transaction_id
      QUALIFY
        1 = ROW_NUMBER() OVER (
          PARTITION BY
            is_trial_period
          ORDER BY
            _source_offset,
            expires_date_ms DESC
        )
    )
  ) AS receipt_info
LEFT JOIN
  UNNEST(
    ARRAY(
      SELECT AS STRUCT
        auto_renew_product_id,
        auto_renew_status,
        expiration_intent,
        COALESCE(is_in_billing_retry_period = 1, FALSE) AS is_in_billing_retry,
      FROM
        UNNEST(apple_receipt.pending_renewal_info)
        WITH OFFSET AS _offset
      WHERE
        latest_original_transaction_id = original_transaction_id
      ORDER BY
        _offset DESC
      LIMIT
        1
    )
  ) AS renewal_info
