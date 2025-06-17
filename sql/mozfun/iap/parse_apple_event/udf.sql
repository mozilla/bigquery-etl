CREATE OR REPLACE FUNCTION iap.parse_apple_event(input STRING) AS (
  -- WARNING: subscription_platform_derived.apple_subscriptions_v1 and
  -- subscription_platform_derived.nonprod_apple_subscriptions_v1 require field order of
  -- mozilla_vpn_derived.guardian_apple_events_v1 to exactly match:
  --   event_timestamp,
  --   mozfun.iap.parse_apple_event(`data`).*,
  STRUCT(
    -- https://developer.apple.com/documentation/appstoreservernotifications/responsebodyv2decodedpayload
    JSON_VALUE(input, "$.autoRenewProductId") AS auto_renew_product_id,
    CAST(JSON_VALUE(input, "$.autoRenewStatus") AS INT64) AS auto_renew_status,
    JSON_VALUE(input, "$.bundleId") AS bundle_id,
    JSON_VALUE(input, "$.currency") AS currency,
    JSON_VALUE(input, "$.environment") AS environment,
    CAST(JSON_VALUE(input, "$.expirationIntent") AS INT64) AS expiration_intent,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.expiresDate") AS INT64)) AS expires_date,
    JSON_VALUE(input, "$.formOfPayment") AS form_of_payment,
    TIMESTAMP_MILLIS(
      CAST(JSON_VALUE(input, "$.gracePeriodExpiresDate") AS INT64)
    ) AS grace_period_expires_date,
    JSON_VALUE(input, "$.inAppOwnershipType") AS in_app_ownership_type,
    CAST(JSON_VALUE(input, "$.isInBillingRetry") AS BOOL) AS is_in_billing_retry,
    CAST(JSON_VALUE(input, "$.isUpgraded") AS BOOL) AS is_upgraded,
    JSON_VALUE(input, "$.latestNotificationSubtype") AS latest_notification_subtype,
    JSON_VALUE(input, "$.latestNotificationType") AS latest_notification_type,
    JSON_VALUE(input, "$.offerIdentifier") AS offer_identifier,
    CAST(JSON_VALUE(input, "$.offerType") AS INT64) AS offer_type,
    TIMESTAMP_MILLIS(
      CAST(JSON_VALUE(input, "$.originalPurchaseDate") AS INT64)
    ) AS original_purchase_date,
    JSON_VALUE(input, "$.originalTransactionId") AS original_transaction_id,
    CAST(JSON_VALUE(input, "$.price") AS INT64) AS price,
    JSON_VALUE(input, "$.productId") AS product_id,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.purchaseDate") AS INT64)) AS purchase_date,
    JSON_VALUE(input, "$.renewalCurrency") AS renewal_currency,
    JSON_VALUE(input, "$.renewalOfferIdentifier") AS renewal_offer_identifier,
    CAST(JSON_VALUE(input, "$.renewalOfferType") AS INT64) AS renewal_offer_type,
    CAST(JSON_VALUE(input, "$.renewalPrice") AS INT64) AS renewal_price,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.revocationDate") AS INT64)) AS revocation_date,
    CAST(JSON_VALUE(input, "$.revocationReason") AS INT64) AS revocation_reason,
    CAST(JSON_VALUE(input, "$.status") AS INT64) AS status,
    JSON_VALUE(input, "$.storefront") AS storefront,
    JSON_VALUE(input, "$.transactionId") AS transaction_id,
    JSON_VALUE(input, "$.type") AS type,
    TO_HEX(SHA256(JSON_VALUE(input, "$.userId"))) AS user_id,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.verifiedAt") AS INT64)) AS verified_at
  )
);

SELECT
  assert.json_equals(
    expected => STRUCT(
      "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS auto_renew_product_id,
      1 AS auto_renew_status,
      "org.mozilla.ios.FirefoxVPN" AS bundle_id,
      "USD" AS currency,
      "Sandbox" AS environment,
      1 AS expiration_intent,
      TIMESTAMP "1970-01-01 00:00:01 UTC" AS expires_date,
      "APPLE_APP_STORE" AS form_of_payment,
      TIMESTAMP "1970-01-01 00:00:02 UTC" AS grace_period_expires_date,
      "PURCHASED" AS in_app_ownership_type,
      FALSE AS is_in_billing_retry,
      TRUE AS is_upgraded,
      "RESUBSCRIBE" AS latest_notification_subtype,
      "SUBSCRIBED" AS latest_notification_type,
      "mozcoupon" AS offer_identifier,
      1 AS offer_type,
      TIMESTAMP "1970-01-01 00:00:03 UTC" AS original_purchase_date,
      "2000000123456789" AS original_transaction_id,
      999 AS price,
      "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS product_id,
      TIMESTAMP "1970-01-01 00:00:04 UTC" AS purchase_date,
      "USD" AS renewal_currency,
      "mozcoupon" AS renewal_offer_identifier,
      1 AS renewal_offer_type,
      999 AS renewal_price,
      TIMESTAMP "1970-01-01 00:00:05 UTC" AS revocation_date,
      0 AS revocation_reason,
      1 AS status,
      "USA" AS storefront,
      "2000000987654321" AS transaction_id,
      "Auto-Renewable Subscription" AS type,
      TO_HEX(SHA256("user id")) AS user_id,
      TIMESTAMP "1970-01-01 00:00:03 UTC" AS verified_at
    ),
    actual => iap.parse_apple_event(
      """
          {
            "autoRenewProductId": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
            "autoRenewStatus": 1,
            "currency": "USD",
            "bundleId": "org.mozilla.ios.FirefoxVPN",
            "environment": "Sandbox",
            "expirationIntent": 1,
            "expiresDate": 1000,
            "formOfPayment": "APPLE_APP_STORE",
            "gracePeriodExpiresDate": 2000,
            "inAppOwnershipType": "PURCHASED",
            "isInBillingRetry": false,
            "isUpgraded": true,
            "latestNotificationSubtype": "RESUBSCRIBE",
            "latestNotificationType": "SUBSCRIBED",
            "offerIdentifier": "mozcoupon",
            "offerType": 1,
            "originalPurchaseDate": 3000,
            "originalTransactionId": 2000000123456789,
            "price": 999,
            "productId": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
            "purchaseDate": 4000,
            "renewalCurrency": "USD",
            "renewalOfferIdentifier": "mozcoupon",
            "renewalOfferType": 1,
            "renewalPrice": 999,
            "revocationDate": 5000,
            "revocationReason": 0,
            "status": 1,
            "storefront": "USA",
            "transactionId": "2000000987654321",
            "type": "Auto-Renewable Subscription",
            "userId": "user id",
            "verifiedAt": 3000
          }
      """
    )
  )
