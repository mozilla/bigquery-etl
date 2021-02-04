CREATE TEMP FUNCTION udf_js_apple_receipt(receipt STRING)
-- provide the full schema even though most of it is scrubbed
RETURNS STRUCT<
  environment STRING,
  latest_receipt BYTES,
  latest_receipt_info ARRAY<
    STRUCT<
      expires_date STRING,
      expires_date_ms STRING,
      expires_date_pst STRING,
      in_app_ownership_type STRING,
      is_in_intro_offer_period STRING,
      is_trial_period STRING,
      original_purchase_date STRING,
      original_purchase_date_ms STRING,
      original_purchase_date_pst STRING,
      original_transaction_id STRING,
      product_id STRING,
      purchase_date STRING,
      purchase_date_ms STRING,
      purchase_date_pst STRING,
      quantity STRING,
      subscription_group_identifier STRING,
      transaction_id STRING,
      web_order_line_item_id STRING
    >
  >,
  pending_renewal_info ARRAY<
    STRUCT<
      auto_renew_product_id STRING,
      auto_renew_status STRING,
      original_transaction_id STRING,
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
        expires_date STRING,
        expires_date_ms STRING,
        expires_date_pst STRING,
        in_app_ownership_type STRING,
        is_in_intro_offer_period STRING,
        is_trial_period STRING,
        original_purchase_date STRING,
        original_purchase_date_ms STRING,
        original_purchase_date_pst STRING,
        original_transaction_id STRING,
        product_id STRING,
        purchase_date STRING,
        purchase_date_ms STRING,
        purchase_date_pst STRING,
        quantity STRING,
        transaction_id STRING,
        web_order_line_item_id STRING
      >
    >,
    original_application_version STRING,
    original_purchase_date STRING,
    original_purchase_date_ms STRING,
    original_purchase_date_pst STRING,
    receipt_creation_date STRING,
    receipt_creation_date_ms STRING,
    receipt_creation_date_pst STRING,
    receipt_type STRING,
    request_date STRING,
    request_date_ms STRING,
    request_date_pst STRING,
    version_external_identifier INT64,
  >,
  status INT64,
> DETERMINISTIC
LANGUAGE js
AS
  "return JSON.parse(receipt);";

CREATE TEMP FUNCTION udf_scrub_apple_receipt(apple_receipt ANY TYPE) AS (
  STRUCT(
    apple_receipt.environment,
    -- extract a list of start and end dates that doesn't overlap or repeat
    ARRAY(
      WITH exploded AS (
        SELECT
          date,
          IF(
            ANY_VALUE(date) OVER (
              ORDER BY
                UNIX_DATE(date) RANGE
                BETWEEN 1 PRECEDING
                AND 1 PRECEDING
            ) IS NULL,
            date,
            NULL
          ) AS start_date,
          IF(
            ANY_VALUE(date) OVER (
              ORDER BY
                UNIX_DATE(date) RANGE
                BETWEEN 1 FOLLOWING
                AND 1 FOLLOWING
            ) IS NULL,
            date,
            NULL
          ) AS end_date,
        FROM
          UNNEST(apple_receipt.receipt.in_app)
        CROSS JOIN
          UNNEST(
            GENERATE_DATE_ARRAY(
              DATE(TIMESTAMP_MILLIS(SAFE_CAST(purchase_date_ms AS INT64))),
              DATE(TIMESTAMP_MILLIS(SAFE_CAST(expires_date_ms AS INT64)))
            )
          ) AS date
        WHERE
          is_trial_period = "false"
      ),
      matched AS (
        SELECT
          start_date,
          MIN(end_date) OVER (
            ORDER BY
              date
            ROWS BETWEEN
              CURRENT ROW
              AND UNBOUNDED FOLLOWING
          ) AS end_date,
        FROM
          exploded
      )
      SELECT AS STRUCT
        *
      FROM
        matched
      WHERE
        start_date IS NOT NULL
    ) AS active_periods
  )
);

SELECT
  user_id,
  is_active,
  created_at,
  ended_at,
  updated_at,
  type,
  provider,
  IF(
    provider = "APPLE",
    udf_scrub_apple_receipt(udf_js_apple_receipt(provider_receipt_json)),
    NULL
  ) AS apple_receipt,
FROM
  mozilla_vpn_external.subscriptions_v1
