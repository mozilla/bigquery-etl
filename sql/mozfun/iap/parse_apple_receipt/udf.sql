CREATE OR REPLACE FUNCTION iap.parse_apple_receipt(provider_receipt_json STRING)
RETURNS STRUCT<
  environment STRING,
  latest_receipt BYTES,
  latest_receipt_info ARRAY<
    STRUCT<
      expires_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      expires_date_ms INT64,
      expires_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      in_app_ownership_type STRING,
      is_in_intro_offer_period STRING,  -- BOOL
      is_trial_period STRING,  -- BOOL
      original_purchase_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      original_purchase_date_ms INT64,
      original_purchase_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      original_transaction_id INT64,
      product_id STRING,
      purchase_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      purchase_date_ms INT64,
      purchase_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
      quantity INT64,
      subscription_group_identifier INT64,
      transaction_id INT64,
      web_order_line_item_id INT64
    >
  >,
  pending_renewal_info ARRAY<
    STRUCT<
      auto_renew_product_id STRING,
      auto_renew_status INT64,
      expiration_intent INT64,
      is_in_billing_retry_period INT64,
      original_transaction_id INT64,
      product_id STRING
    >
  >,
  receipt STRUCT<
    adam_id INT64,
    app_item_id INT64,
    application_version STRING,
    bundle_id STRING,
    download_id INT64,
    in_app ARRAY<
      STRUCT<
        expires_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        expires_date_ms INT64,
        expires_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        in_app_ownership_type STRING,
        is_in_intro_offer_period STRING,  -- BOOL
        is_trial_period STRING,  -- BOOL
        original_purchase_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        original_purchase_date_ms INT64,
        original_purchase_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        original_transaction_id INT64,
        product_id STRING,
        purchase_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        purchase_date_ms INT64,
        purchase_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
        quantity INT64,
        transaction_id INT64,
        web_order_line_item_id INT64
      >
    >,
    original_application_version STRING,
    original_purchase_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    original_purchase_date_ms INT64,
    original_purchase_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    receipt_creation_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    receipt_creation_date_ms INT64,
    receipt_creation_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    receipt_type STRING,
    request_date STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    request_date_ms INT64,
    request_date_pst STRING,  -- TIMESTAMP "%Y-%m-%d %H:%M:%E*S %Z"
    version_external_identifier INT64
  >,
  status INT64
> DETERMINISTIC
LANGUAGE js
AS
  "return JSON.parse(provider_receipt_json);";

SELECT
  assert.json_equals(
    STRUCT(
      "Sandbox" AS environment,
      b"test" AS latest_receipt,
      [
        STRUCT(
          "2021-01-08 02:10:25 Etc/GMT" AS expires_date,
          1610071825000 AS expires_date_ms,
          "2021-01-07 18:10:25 America/Los_Angeles" AS expires_date_pst,
          "PURCHASED" AS in_app_ownership_type,
          "false" AS is_in_intro_offer_period,
          "false" AS is_trial_period,
          "2021-01-08 01:40:27 Etc/GMT" AS original_purchase_date,
          1610070027000 AS original_purchase_date_ms,
          "2021-01-07 17:40:27 America/Los_Angeles" AS original_purchase_date_pst,
          1000000762442282 AS original_transaction_id,
          "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS product_id,
          "2021-01-08 02:05:25 Etc/GMT" AS purchase_date,
          1610071525000 AS purchase_date_ms,
          "2021-01-07 18:05:25 America/Los_Angeles" AS purchase_date_pst,
          1 AS quantity,
          20693686 AS subscription_group_identifier,
          1000000762449239 AS transaction_id,
          1000000058912345 AS web_order_line_item_id
        )
      ] AS latest_receipt_info,
      [
        STRUCT(
          "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS auto_renew_product_id,
          0 AS auto_renew_status,
          1 AS expiration_intent,
          0 AS is_in_billing_retry_period,
          1000000762442282 AS original_transaction_id,
          "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS product_id
        )
      ] AS pending_renewal_info,
      STRUCT(
        0 AS adam_id,
        0 AS app_item_id,
        "2.202101062113" AS application_version,
        "org.mozilla.ios.FirefoxVPN" AS bundle_id,
        0 AS download_id,
        [
          STRUCT(
            "2021-01-08 01:45:25 Etc/GMT" AS expires_date,
            1610070325000 AS expires_date_ms,
            "2021-01-07 17:45:25 America/Los_Angeles" AS expires_date_pst,
            "PURCHASED" AS in_app_ownership_type,
            "false" AS is_in_intro_offer_period,
            "false" AS is_trial_period,
            "2021-01-08 01:40:27 Etc/GMT" AS original_purchase_date,
            1610070027000 AS original_purchase_date_ms,
            "2021-01-07 17:40:27 America/Los_Angeles" AS original_purchase_date_pst,
            1000000762442282 AS original_transaction_id,
            "org.mozilla.ios.FirefoxVPN.product.1_month_subscription" AS product_id,
            "2021-01-08 01:40:25 Etc/GMT" AS purchase_date,
            1610070025000 AS purchase_date_ms,
            "2021-01-07 17:40:25 America/Los_Angeles" AS purchase_date_pst,
            1 AS quantity,
            1000000762442282 AS transaction_id,
            1000000058912047 AS web_order_line_item_id
          )
        ] AS in_app,
        "1.0" AS original_application_version,
        "2013-08-01 07:00:00 Etc/GMT" AS original_purchase_date,
        1375340400000 AS original_purchase_date_ms,
        "2013-08-01 00:00:00 America/Los_Angeles" AS original_purchase_date_pst,
        "2021-01-08 01:40:28 Etc/GMT" AS receipt_creation_date,
        1610070028000 AS receipt_creation_date_ms,
        "2021-01-07 17:40:28 America/Los_Angeles" AS receipt_creation_date_pst,
        "ProductionSandbox" AS receipt_type,
        "2021-01-28 23:25:51 Etc/GMT" AS request_date,
        1611876351696 AS request_date_ms,
        "2021-01-28 15:25:51 America/Los_Angeles" AS request_date_pst,
        0 AS version_external_identifier
      ) AS receipt,
      0 AS status
    ),
    iap.parse_apple_receipt(
      """
        {
          "status": 0,
          "receipt": {
            "in_app": [
              {
                "quantity": "1",
                "product_id": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
                "expires_date": "2021-01-08 01:45:25 Etc/GMT",
                "purchase_date": "2021-01-08 01:40:25 Etc/GMT",
                "transaction_id": 1000000762442282,
                "expires_date_ms": 1610070325000,
                "is_trial_period": "false",
                "expires_date_pst": "2021-01-07 17:45:25 America/Los_Angeles",
                "purchase_date_ms": 1610070025000,
                "purchase_date_pst": "2021-01-07 17:40:25 America/Los_Angeles",
                "in_app_ownership_type": "PURCHASED",
                "original_purchase_date": "2021-01-08 01:40:27 Etc/GMT",
                "web_order_line_item_id": 1000000058912047,
                "original_transaction_id": 1000000762442282,
                "is_in_intro_offer_period": "false",
                "original_purchase_date_ms": 1610070027000,
                "original_purchase_date_pst": "2021-01-07 17:40:27 America/Los_Angeles"
              }
            ],
            "adam_id": 0,
            "bundle_id": "org.mozilla.ios.FirefoxVPN",
            "app_item_id": 0,
            "download_id": 0,
            "receipt_type": "ProductionSandbox",
            "request_date": "2021-01-28 23:25:51 Etc/GMT",
            "request_date_ms": "1611876351696",
            "request_date_pst": "2021-01-28 15:25:51 America/Los_Angeles",
            "application_version": "2.202101062113",
            "receipt_creation_date": "2021-01-08 01:40:28 Etc/GMT",
            "original_purchase_date": "2013-08-01 07:00:00 Etc/GMT",
            "receipt_creation_date_ms": "1610070028000",
            "original_purchase_date_ms": "1375340400000",
            "receipt_creation_date_pst": "2021-01-07 17:40:28 America/Los_Angeles",
            "original_purchase_date_pst": "2013-08-01 00:00:00 America/Los_Angeles",
            "version_external_identifier": 0,
            "original_application_version": "1.0"
          },
          "environment": "Sandbox",
          "latest_receipt": "dGVzdA==",
          "latest_receipt_info": [
            {
              "quantity": "1",
              "product_id": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
              "expires_date": "2021-01-08 02:10:25 Etc/GMT",
              "purchase_date": "2021-01-08 02:05:25 Etc/GMT",
              "transaction_id": "1000000762449239",
              "expires_date_ms": "1610071825000",
              "is_trial_period": "false",
              "expires_date_pst": "2021-01-07 18:10:25 America/Los_Angeles",
              "purchase_date_ms": "1610071525000",
              "purchase_date_pst": "2021-01-07 18:05:25 America/Los_Angeles",
              "in_app_ownership_type": "PURCHASED",
              "original_purchase_date": "2021-01-08 01:40:27 Etc/GMT",
              "web_order_line_item_id": "1000000058912345",
              "original_transaction_id": "1000000762442282",
              "is_in_intro_offer_period": "false",
              "original_purchase_date_ms": "1610070027000",
              "original_purchase_date_pst": "2021-01-07 17:40:27 America/Los_Angeles",
              "subscription_group_identifier": "20693686"
            }
          ],
          "pending_renewal_info": [
            {
              "product_id": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
              "auto_renew_status": "0",
              "expiration_intent": "1",
              "auto_renew_product_id": "org.mozilla.ios.FirefoxVPN.product.1_month_subscription",
              "original_transaction_id": "1000000762442282",
              "is_in_billing_retry_period": "0"
            }
          ]
        }
      """
    )
  )
