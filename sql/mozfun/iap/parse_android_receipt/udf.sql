CREATE OR REPLACE FUNCTION iap.parse_android_receipt(input STRING) AS (
  STRUCT(
    -- https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.subscriptions
    CAST(JSON_VALUE(input, "$.acknowledgementState") AS INT64) AS acknowledgement_state,
    CAST(JSON_VALUE(input, "$.autoRenewing") AS BOOL) AS auto_renewing,
    TIMESTAMP_MILLIS(
      CAST(JSON_VALUE(input, "$.autoResumeTimeMillis") AS INT64)
    ) AS auto_resume_time,
    CAST(JSON_VALUE(input, "$.cancelReason") AS INT64) AS cancel_reason,
    IF(
      JSON_QUERY(input, "$.cancelSurveyResult") IS NOT NULL,
      STRUCT(
        CAST(
          JSON_VALUE(input, "$.cancelSurveyResult.cancelSurveyReason") AS INT64
        ) AS cancel_survey_reason,
        JSON_VALUE(input, "$.cancelSurveyResult.userInputCancelReason") AS user_input_cancel_reason
      ),
      NULL
    ) AS cancel_survey_result,
    JSON_VALUE(input, "$.countryCode") AS country_code,
    JSON_VALUE(input, "$.developerPayload") AS developer_payload,
    JSON_VALUE(input, "$.emailAddress") AS email_address,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.expiryTimeMillis") AS INT64)) AS expiry_time,
    JSON_VALUE(input, "$.externalAccountId") AS external_account_id,
    JSON_VALUE(input, "$.familyName") AS family_name,
    JSON_VALUE(input, "$.givenName") AS given_name,
    IF(
      JSON_QUERY(input, "$.introductoryPriceInfo") IS NOT NULL,
      STRUCT(
        CAST(
          JSON_VALUE(input, "$.introductoryPriceInfo.introductoryPriceAmountMicros") AS INT64
        ) AS introductory_price_amount_micros,
        JSON_VALUE(
          input,
          "$.introductoryPriceInfo.introductoryPriceCurrencyCode"
        ) AS introductory_price_currency_code,
        CAST(
          JSON_VALUE(input, "$.introductoryPriceInfo.introductoryPriceCycles") AS INT64
        ) AS introductory_price_cycles,
        JSON_VALUE(
          input,
          "$.introductoryPriceInfo.introductoryPricePeriod"
        ) AS introductory_price_period
      ),
      NULL
    ) AS introductory_price_info,
    JSON_VALUE(input, "$.kind") AS kind,
    JSON_VALUE(input, "$.linkedPurchaseToken") AS linked_purchase_token,
    JSON_VALUE(input, "$.obfuscatedExternalAccountId") AS obfuscated_external_account_id,
    JSON_VALUE(input, "$.obfuscatedExternalProfileId") AS obfuscated_external_profile_id,
    JSON_VALUE(input, "$.orderId") AS order_id,
    CAST(JSON_VALUE(input, "$.paymentState") AS INT64) AS payment_state,
    CAST(JSON_VALUE(input, "$.priceAmountMicros") AS INT64) AS price_amount_micros,
    IF(
      JSON_QUERY(input, "$.priceChange") IS NOT NULL,
      STRUCT(
        IF(
          JSON_QUERY(input, "$.priceChange.newPrice") IS NOT NULL,
          STRUCT(
            JSON_VALUE(input, "$.priceChange.newPrice.currency") AS currency,
            CAST(JSON_VALUE(input, "$.priceChange.newPrice.priceMicros") AS INT64) AS price_micros
          ),
          NULL
        ) AS new_price,
        CAST(JSON_VALUE(input, "$.priceChange.state") AS INT64) AS state
      ),
      NULL
    ) AS price_change,
    JSON_VALUE(input, "$.priceCurrencyCode") AS price_currency_code,
    JSON_VALUE(input, "$.profileId") AS profile_id,
    JSON_VALUE(input, "$.profileName") AS profile_name,
    JSON_VALUE(input, "$.promotionCode") AS promotion_code,
    CAST(JSON_VALUE(input, "$.promotionType") AS INT64) AS promotion_type,
    CAST(JSON_VALUE(input, "$.purchaseType") AS INT64) AS purchase_type,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.startTimeMillis") AS INT64)) AS start_time,
    TIMESTAMP_MILLIS(
      CAST(JSON_VALUE(input, "$.userCancellationTimeMillis") AS INT64)
    ) AS user_cancellation_time,
    -- Added by FxA
    JSON_VALUE(input, "$.formOfPayment") AS form_of_payment,
    CAST(JSON_VALUE(input, "$.isMutable") AS BOOL) AS is_mutable,
    CAST(JSON_VALUE(input, "$.latestNotificationType") AS INT64) AS latest_notification_type,
    JSON_VALUE(input, "$.packageName") AS package_name,
    JSON_VALUE(input, "$.purchaseToken") AS purchase_token,
    CAST(JSON_VALUE(input, "$.replacedByAnotherPurchase") AS BOOL) AS replaced_by_another_purchase,
    JSON_VALUE(input, "$.sku") AS sku,
    JSON_VALUE(input, "$.skuType") AS sku_type,
    TO_HEX(SHA256(JSON_VALUE(input, "$.userId"))) AS user_id,
    TIMESTAMP_MILLIS(CAST(JSON_VALUE(input, "$.verifiedAt") AS INT64)) AS verified_at
  )
);

SELECT
  assert.json_equals(
    expected => STRUCT(
      1 AS acknowledgement_state,
      FALSE AS auto_renewing,
      TIMESTAMP "1970-01-01 00:00:04 UTC" AS auto_resume_time,
      0 AS cancel_reason,
      STRUCT(1 AS cancel_survey_reason, "test" AS user_input_cancel_reason) AS cancel_survey_result,
      "US" AS country_code,
      "dev payload" AS developer_payload,
      "email" AS email_address,
      TIMESTAMP "1970-01-01 00:00:02 UTC" AS expiry_time,
      "ext acct" AS external_account_id,
      "fname" AS family_name,
      "gname" AS given_name,
      STRUCT(
        1000000 AS introductory_price_amount_micros,
        "USD" AS introductory_price_currency_code,
        1 AS introductory_price_cycles,
        "P1M" AS introductory_price_period
      ) AS introductory_price_info,
      "androidpublisher#subscriptionPurchase" AS kind,
      "linked purchase token" AS linked_purchase_token,
      "obf ext acct" AS obfuscated_external_account_id,
      "obf ext prof" AS obfuscated_external_profile_id,
      "order id" AS order_id,
      1 AS payment_state,
      5990000 AS price_amount_micros,
      STRUCT(
        STRUCT("USD" AS currency, 9990000 AS price_micros) AS new_price,
        0 AS state
      ) AS price_change,
      "USD" AS price_currency_code,
      "prof" AS profile_id,
      "pname" AS profile_name,
      "pcode" AS promotion_code,
      0 AS promotion_type,
      0 AS purchase_type,
      TIMESTAMP "1970-01-01 00:00:00 UTC" AS start_time,
      TIMESTAMP "1970-01-01 00:00:01 UTC" AS user_cancellation_time,
      "GOOGLE_PLAY" AS form_of_payment,
      TRUE AS is_mutable,
      3 AS latest_notification_type,
      "org.mozilla.firefox.vpn" AS package_name,
      "purchase token" AS purchase_token,
      FALSE AS replaced_by_another_purchase,
      "org.mozilla.gps.mozillavpn.product.1_month_subscription" AS sku,
      "subs" AS sku_type,
      TO_HEX(SHA256("user id")) AS user_id,
      TIMESTAMP "1970-01-01 00:00:03 UTC" AS verified_at
    ),
    actual => iap.parse_android_receipt(
      """
          {
            "acknowledgementState": 1,
            "autoRenewing": false,
            "autoResumeTimeMillis": 4000,
            "cancelReason": 0,
            "cancelSurveyResult": {
              "cancelSurveyReason": 1,
              "userInputCancelReason": "test"
            },
            "countryCode": "US",
            "developerPayload": "dev payload",
            "emailAddress": "email",
            "expiryTimeMillis": 2000,
            "externalAccountId": "ext acct",
            "familyName": "fname",
            "givenName": "gname",
            "introductoryPriceInfo": {
              "introductoryPriceAmountMicros": "1000000",
              "introductoryPriceCurrencyCode": "USD",
              "introductoryPriceCycles": 1,
              "introductoryPricePeriod": "P1M"
            },
            "kind": "androidpublisher#subscriptionPurchase",
            "linkedPurchaseToken": "linked purchase token",
            "obfuscatedExternalAccountId": "obf ext acct",
            "obfuscatedExternalProfileId": "obf ext prof",
            "orderId": "order id",
            "paymentState": 1,
            "priceAmountMicros": 5990000,
            "priceChange": {
              "newPrice": {
                "currency": "USD",
                "priceMicros": "9990000",
              },
              "state": 0
            },
            "priceCurrencyCode": "USD",
            "profileId": "prof",
            "profileName": "pname",
            "promotionCode": "pcode",
            "promotionType": 0,
            "purchaseType": 0,
            "startTimeMillis": 0,
            "userCancellationTimeMillis": 1000,
            "formOfPayment": "GOOGLE_PLAY",
            "isMutable": true,
            "latestNotificationType": 3,
            "packageName": "org.mozilla.firefox.vpn",
            "purchaseToken": "purchase token",
            "replacedByAnotherPurchase": false,
            "sku": "org.mozilla.gps.mozillavpn.product.1_month_subscription",
            "skuType": "subs",
            "userId": "user id",
            "verifiedAt": 3000
          }
        """
    )
  )
