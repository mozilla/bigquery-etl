WITH customers AS (
  -- Get customer records added or updated since the last run.
  SELECT
    id,
    _fivetran_synced,
    address_country,
    address_postal_code,
    address_state,
    created,
    default_card_id,
    is_deleted,
    metadata,
    shipping_address_country,
    shipping_address_postal_code,
    shipping_address_state,
    source_id,
    tax_exempt,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.customer_v1
  WHERE
    _fivetran_synced > COALESCE(
      (
        SELECT
          MAX(`timestamp`)
        FROM
          `moz-fx-data-shared-prod`.stripe_external.customers_changelog_v1
      ),
      '0001-01-01 00:00:00'
    )
),
customer_latest_discounts AS (
  SELECT
    customers.id AS customer_id,
    ARRAY_AGG(
      STRUCT(
        customer_discounts.id,
        STRUCT(
          customer_discounts.coupon_id AS id,
          coupons.amount_off,
          coupons.created,
          coupons.currency,
          coupons.duration,
          coupons.duration_in_months,
          metadata,
          coupons.name,
          coupons.percent_off,
          coupons.redeem_by
        ) AS coupon,
        customer_discounts.`end`,
        customer_discounts.invoice_id,
        customer_discounts.invoice_item_id,
        customer_discounts.promotion_code AS promotion_code_id,
        customer_discounts.start,
        customer_discounts.subscription_id
      )
      ORDER BY
        customer_discounts.start DESC
      LIMIT
        1
    )[SAFE_ORDINAL(1)] AS discount
  FROM
    customers
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.customer_discount_v1 AS customer_discounts
  ON
    customers.id = customer_discounts.customer_id
    AND customers._fivetran_synced >= customer_discounts.start
    AND (customers._fivetran_synced < customer_discounts.`end` OR customer_discounts.`end` IS NULL)
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.coupon_v1 AS coupons
  ON
    customer_discounts.coupon_id = coupons.id
  GROUP BY
    customers.id
)
SELECT
  CONCAT(customers.id, '-', FORMAT_TIMESTAMP('%FT%H:%M:%E6S', customers._fivetran_synced)) AS id,
  customers._fivetran_synced AS `timestamp`,
  STRUCT(
    customers.id,
    STRUCT(
      customers.address_country AS country,
      customers.address_postal_code AS postal_code,
      customers.address_state AS state
    ) AS address,
    customers.created,
    COALESCE(customers.source_id, customers.default_card_id) AS default_source_id,
    customer_latest_discounts.discount,
    customers.is_deleted,
    customers.metadata,
    STRUCT(
      STRUCT(
        customers.shipping_address_country AS country,
        customers.shipping_address_postal_code AS postal_code,
        customers.shipping_address_state AS state
      ) AS address
    ) AS shipping,
    customers.tax_exempt
  ) AS customer
FROM
  customers
LEFT JOIN
  customer_latest_discounts
ON
  customers.id = customer_latest_discounts.customer_id
