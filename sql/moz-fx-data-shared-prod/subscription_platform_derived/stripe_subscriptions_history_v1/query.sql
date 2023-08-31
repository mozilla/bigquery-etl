WITH base_subscriptions_history AS (
  SELECT
    customer_id,
    id AS subscription_id,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_start) AS subscription_history_row_number,
    _fivetran_synced AS synced_at,
    _fivetran_start AS valid_from,
    LEAD(_fivetran_start) OVER (PARTITION BY id ORDER BY _fivetran_start) AS valid_to,
    created,
    trial_start,
    trial_end,
    COALESCE(trial_end, start_date) AS subscription_start_date,
    cancel_at,
    cancel_at_period_end,
    canceled_at,
    JSON_VALUE(metadata, "$.cancelled_for_customer_at") AS canceled_for_customer_at,
    ended_at,
    status,
    TIMESTAMP_SECONDS(
      CAST(JSON_VALUE(metadata, "$.plan_change_date") AS INT64)
    ) AS plan_change_date,
    JSON_VALUE(metadata, "$.previous_plan_id") AS previous_plan_id,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscription_history_v1
),
subscriptions_history AS (
  -- If a subscription's first history record has a previous_plan_id then synthesize
  -- an extra history record to represent that previous plan period.
  SELECT
    * REPLACE (
      0 AS subscription_history_row_number,
      plan_change_date AS valid_to,
      CAST(NULL AS TIMESTAMP) AS cancel_at,
      FALSE AS cancel_at_period_end,
      CAST(NULL AS TIMESTAMP) AS canceled_at,
      CAST(NULL AS STRING) AS canceled_for_customer_at,
      CAST(NULL AS TIMESTAMP) AS ended_at,
      "active" AS status,
      CAST(NULL AS TIMESTAMP) AS plan_change_date,
      CAST(NULL AS STRING) AS previous_plan_id
    )
  FROM
    base_subscriptions_history
  WHERE
    subscription_history_row_number = 1
    AND plan_change_date IS NOT NULL
    AND previous_plan_id IS NOT NULL
  UNION ALL
  SELECT
    * REPLACE (
      IF(
        subscription_history_row_number = 1,
        COALESCE(plan_change_date, valid_from),
        valid_from
      ) AS valid_from
    )
  FROM
    base_subscriptions_history
),
subscriptions_history_with_lead_plan_metadata AS (
  SELECT
    *,
    LEAD(plan_change_date) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        valid_from
    ) AS lead_plan_change_date,
    LEAD(previous_plan_id) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        valid_from
    ) AS lead_previous_plan_id,
  FROM
    subscriptions_history
),
subscriptions_history_with_plan_ends AS (
  SELECT
    *,
    -- A new `previous_plan_id` value means the previous row was the last row with that plan.
    IF(
      lead_previous_plan_id IS DISTINCT FROM previous_plan_id,
      STRUCT(lead_previous_plan_id AS plan_id, lead_plan_change_date AS plan_ended_at),
      (NULL, NULL)
    ).*
  FROM
    subscriptions_history_with_lead_plan_metadata
),
subscriptions_history_with_previous_plan_ids AS (
  -- Fill in previous `plan_id` values by looking forward to the next non-null `plan_id`.
  SELECT
    * EXCEPT (plan_id),
    FIRST_VALUE(plan_id IGNORE NULLS) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        valid_from
      ROWS BETWEEN
        CURRENT ROW
        AND UNBOUNDED FOLLOWING
    ) AS plan_id,
    LAST_VALUE(plan_ended_at IGNORE NULLS) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        valid_from
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND 1 PRECEDING
    ) AS plan_started_at,
  FROM
    subscriptions_history_with_plan_ends
),
subscription_items AS (
  SELECT
    id AS subscription_item_id,
    subscription_id,
    plan_id,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscription_item_v1
  QUALIFY
    -- With how our subscription platform currently works each Stripe subscription should
    -- only have one subscription item, and we enforce that so the ETL can rely on it.
    1 = COUNT(*) OVER (PARTITION BY subscription_id)
),
subscriptions_history_with_plan_ids AS (
  -- Fill in current `plan_id` values from subscription items.
  SELECT
    subscriptions_history.* REPLACE (
      COALESCE(subscriptions_history.plan_id, subscription_items.plan_id) AS plan_id
    ),
    subscription_items.subscription_item_id,
  FROM
    subscriptions_history_with_previous_plan_ids AS subscriptions_history
  JOIN
    subscription_items
    USING (subscription_id)
),
product_capabilities AS (
  SELECT
    products.id AS product_id,
    ARRAY_AGG(DISTINCT TRIM(capability) IGNORE NULLS) AS capabilities
  FROM
    `moz-fx-data-shared-prod`.stripe_external.product_v1 AS products
  JOIN
    UNNEST(mozfun.json.js_extract_string_map(metadata)) AS metadata_items
    ON metadata_items.key LIKE 'capabilities%'
  JOIN
    UNNEST(SPLIT(metadata_items.value, ",")) AS capability
  WHERE
    TRIM(capability) != products.id
  GROUP BY
    product_id
),
plan_capabilities AS (
  SELECT
    plans.id AS plan_id,
    ARRAY_AGG(DISTINCT TRIM(capability) IGNORE NULLS) AS capabilities
  FROM
    `moz-fx-data-shared-prod`.stripe_external.plan_v1 AS plans
  JOIN
    UNNEST(mozfun.json.js_extract_string_map(metadata)) AS metadata_items
    ON metadata_items.key LIKE 'capabilities%'
  JOIN
    UNNEST(SPLIT(metadata_items.value, ",")) AS capability
  WHERE
    TRIM(capability) != plans.product_id
  GROUP BY
    plan_id
),
plans AS (
  SELECT
    plans.id AS plan_id,
    plans.nickname AS plan_name,
    plan_capabilities.capabilities AS plan_capabilities,
    plans.amount AS plan_amount,
    plans.billing_scheme AS billing_scheme,
    plans.currency AS plan_currency,
    plans.interval AS plan_interval,
    plans.interval_count AS plan_interval_count,
    plans.product_id,
    products.name AS product_name,
    product_capabilities.capabilities AS product_capabilities,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.plan_v1 AS plans
  LEFT JOIN
    `moz-fx-data-shared-prod`.stripe_external.product_v1 AS products
    ON plans.product_id = products.id
  LEFT JOIN
    plan_capabilities
    ON plans.id = plan_capabilities.plan_id
  LEFT JOIN
    product_capabilities
    USING (product_id)
),
customers AS (
  SELECT
    id AS customer_id,
    COALESCE(
      TO_HEX(SHA256(JSON_VALUE(customers.metadata, "$.userid"))),
      JSON_VALUE(pre_fivetran_customers.metadata, "$.fxa_uid")
    ) AS fxa_uid,
    COALESCE(customers.address_country, pre_fivetran_customers.address_country) AS address_country,
    customers.shipping_address_country,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.customer_v1 AS customers
  FULL JOIN
    -- Include customers that were deleted before the initial Fivetran Stripe import.
    `moz-fx-data-shared-prod`.stripe_external.pre_fivetran_customers_v1 AS pre_fivetran_customers
    USING (id)
),
charges AS (
  SELECT
    charges.id AS charge_id,
    COALESCE(cards.country, charges.billing_detail_address_country) AS country,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.charge_v1 AS charges
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.card_v1 AS cards
    ON charges.card_id = cards.id
  WHERE
    charges.status = "succeeded"
),
invoices_provider_country AS (
  SELECT
    invoices.subscription_id,
    IF(
      JSON_VALUE(invoices.metadata, "$.paypalTransactionId") IS NOT NULL,
      -- FxA copies PayPal billing agreement country to customer address.
      STRUCT("Paypal" AS provider, customers.address_country AS country),
      ("Stripe", charges.country)
    ).*,
    invoices.created,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.invoice_v1 AS invoices
  LEFT JOIN
    customers
    USING (customer_id)
  LEFT JOIN
    charges
    USING (charge_id)
  WHERE
    invoices.status = "paid"
),
subscriptions_history_invoice_provider_country AS (
  SELECT
    subscriptions_history.subscription_id,
    subscriptions_history.valid_from,
    ARRAY_AGG(
      STRUCT(invoices_provider_country.provider, invoices_provider_country.country)
      ORDER BY
        -- prefer rows with country
        IF(invoices_provider_country.country IS NULL, 0, 1) DESC,
        invoices_provider_country.created DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    subscriptions_history
  JOIN
    invoices_provider_country
    ON subscriptions_history.subscription_id = invoices_provider_country.subscription_id
    AND (
      invoices_provider_country.created < subscriptions_history.valid_to
      OR subscriptions_history.valid_to IS NULL
    )
  GROUP BY
    subscription_id,
    valid_from
),
subscriptions_history_promotions AS (
  SELECT
    subscriptions_history.subscription_id,
    subscriptions_history.valid_from,
    ARRAY_AGG(DISTINCT promotion_codes.code IGNORE NULLS) AS promotion_codes,
    SUM(
      COALESCE(coupons.amount_off, 0) + COALESCE(
        CAST((invoices.subtotal * coupons.percent_off / 100) AS INT64),
        0
      )
    ) AS promotion_discounts_amount,
  FROM
    subscriptions_history
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.invoice_v1 AS invoices
    ON subscriptions_history.subscription_id = invoices.subscription_id
    AND (
      invoices.created < subscriptions_history.valid_to
      OR subscriptions_history.valid_to IS NULL
    )
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.invoice_discount_v1 AS invoice_discounts
    ON invoices.id = invoice_discounts.invoice_id
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.promotion_code_v1 AS promotion_codes
    ON invoice_discounts.promotion_code = promotion_codes.id
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.coupon_v1 AS coupons
    ON promotion_codes.coupon_id = coupons.id
  WHERE
    invoices.status = "paid"
  GROUP BY
    subscription_id,
    valid_from
)
SELECT
  subscriptions_history.customer_id,
  customers.fxa_uid,
  subscriptions_history.subscription_id,
  subscriptions_history.subscription_item_id,
  subscriptions_history.synced_at,
  subscriptions_history.valid_from,
  subscriptions_history.valid_to,
  subscriptions_history.created,
  subscriptions_history.trial_start,
  subscriptions_history.trial_end,
  subscriptions_history.subscription_start_date,
  subscriptions_history.cancel_at,
  subscriptions_history.cancel_at_period_end,
  subscriptions_history.canceled_at,
  subscriptions_history.canceled_for_customer_at,
  subscriptions_history.ended_at,
  subscriptions_history.status,
  plans.product_id,
  plans.product_name,
  plans.product_capabilities,
  subscriptions_history.plan_id,
  subscriptions_history.plan_started_at,
  subscriptions_history.plan_ended_at,
  plans.plan_name,
  plans.plan_capabilities,
  plans.plan_amount,
  plans.billing_scheme,
  plans.plan_currency,
  plans.plan_interval,
  plans.plan_interval_count,
  "Etc/UTC" AS plan_interval_timezone,
  subscriptions_history_invoice_provider_country.provider,
  LOWER(
    -- Use the same address hierarchy as Stripe Tax after we enabled Stripe Tax (FXA-5457).
    -- https://stripe.com/docs/tax/customer-locations#address-hierarchy
    IF(
      (
        DATE(subscriptions_history.valid_to) >= "2022-12-01"
        OR subscriptions_history.valid_to IS NULL
      )
      AND (
        DATE(subscriptions_history.ended_at) >= "2022-12-01"
        OR subscriptions_history.ended_at IS NULL
      ),
      COALESCE(
        NULLIF(customers.shipping_address_country, ""),
        NULLIF(customers.address_country, ""),
        subscriptions_history_invoice_provider_country.country
      ),
      subscriptions_history_invoice_provider_country.country
    )
  ) AS country,
  subscriptions_history_promotions.promotion_codes,
  subscriptions_history_promotions.promotion_discounts_amount,
FROM
  subscriptions_history_with_plan_ids AS subscriptions_history
LEFT JOIN
  plans
  USING (plan_id)
LEFT JOIN
  customers
  USING (customer_id)
LEFT JOIN
  subscriptions_history_invoice_provider_country
  USING (subscription_id, valid_from)
LEFT JOIN
  subscriptions_history_promotions
  USING (subscription_id, valid_from)
