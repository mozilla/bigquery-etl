WITH subscriptions_history AS (
  SELECT
    -- Synthesize a primary key column to make identifying rows and doing joins easier.
    CONCAT(id, '-', FORMAT_TIMESTAMP('%FT%H:%M:%E6S', _fivetran_start)) AS id,
    id AS subscription_id,
    _fivetran_synced,
    _fivetran_start,
    _fivetran_active,
    billing_cycle_anchor,
    cancel_at,
    cancel_at_period_end,
    canceled_at,
    -- `billing` was renamed `collection_method`.
    COALESCE(collection_method, billing) AS collection_method,
    created,
    current_period_end,
    current_period_start,
    customer_id,
    days_until_due,
    default_payment_method_id,
    default_source_id,
    ended_at,
    latest_invoice_id,
    metadata,
    pending_setup_intent_id,
    start_date,
    status,
    trial_end,
    trial_start,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscription_history_v1
  WHERE
    {% if is_init() %}
      TRUE
    {% else %}
      DATE(_fivetran_start) >= @date
    {% endif %}
),
subscription_items AS (
  SELECT
    id,
    created,
    metadata,
    plan_id,
    quantity,
    subscription_id,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscription_item_v1
  QUALIFY
    -- With how SubPlat currently works each Stripe subscription should only have
    -- one subscription item, and we enforce that so the ETL can rely on it.
    1 = COUNT(*) OVER (PARTITION BY subscription_id)
),
products AS (
  SELECT
    id,
    created,
    description,
    metadata,
    name,
    statement_descriptor,
    updated,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.product_v1
),
plans AS (
  SELECT
    id,
    aggregate_usage,
    amount,
    billing_scheme,
    created,
    currency,
    `interval`,
    interval_count,
    metadata,
    nickname,
    product_id,
    tiers_mode,
    trial_period_days,
    usage_type,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.plan_v1
),
subscriptions_history_with_plan_metadata AS (
  SELECT
    *,
    JSON_VALUE(metadata.previous_plan_id) AS previous_plan_id,
    LEAD(JSON_VALUE(metadata.previous_plan_id)) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        _fivetran_start
    ) AS lead_previous_plan_id
  FROM
    subscriptions_history
),
subscriptions_history_with_end_plan_ids AS (
  -- Determine the plan ID for the last record in each time span that a subscription had a particular plan.
  SELECT
    subscriptions_history.*,
    CASE
      -- The latest history record gets the current plan from its associated subscription item.
      WHEN subscriptions_history._fivetran_active
        THEN subscription_items.plan_id
      -- A new `previous_plan_id` value means the previous record was the last record with that plan.
      WHEN subscriptions_history.lead_previous_plan_id IS DISTINCT FROM subscriptions_history.previous_plan_id
        THEN subscriptions_history.lead_previous_plan_id
      ELSE NULL
    END AS end_plan_id
  FROM
    subscriptions_history_with_plan_metadata AS subscriptions_history
  JOIN
    subscription_items
  ON
    subscriptions_history.subscription_id = subscription_items.subscription_id
),
subscriptions_history_with_plan_ids AS (
  -- Fill in `plan_id` by getting the next `end_plan_id`.
  SELECT
    *,
    FIRST_VALUE(end_plan_id IGNORE NULLS) OVER (
      PARTITION BY
        subscription_id
      ORDER BY
        _fivetran_start
      ROWS BETWEEN
        CURRENT ROW
        AND UNBOUNDED FOLLOWING
    ) AS plan_id
  FROM
    subscriptions_history_with_end_plan_ids
),
subscriptions_history_tax_rates AS (
  SELECT
    subscriptions_history.id AS subscription_history_id,
    ARRAY_AGG(
      STRUCT(
        subscription_tax_rates.tax_rate_id AS id,
        tax_rates.created,
        tax_rates.description,
        tax_rates.display_name,
        tax_rates.inclusive,
        tax_rates.jurisdiction,
        tax_rates.metadata,
        tax_rates.percentage
      )
      ORDER BY
        tax_rates.created
    ) AS tax_rates
  FROM
    subscriptions_history
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.subscription_tax_rate_v1 AS subscription_tax_rates
  ON
    subscriptions_history.subscription_id = subscription_tax_rates.subscription_id
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.tax_rate_v1 AS tax_rates
  ON
    subscription_tax_rates.tax_rate_id = tax_rates.id
    AND subscriptions_history._fivetran_start >= tax_rates.created
  GROUP BY
    subscriptions_history.id
),
subscriptions_history_latest_discounts AS (
  SELECT
    subscriptions_history.id AS subscription_history_id,
    ARRAY_AGG(
      STRUCT(
        subscription_discounts.id,
        STRUCT(
          subscription_discounts.coupon_id AS id,
          coupons.amount_off,
          coupons.created,
          coupons.currency,
          coupons.duration,
          coupons.duration_in_months,
          coupons.metadata,
          coupons.name,
          coupons.percent_off,
          coupons.redeem_by
        ) AS coupon,
        subscription_discounts.`end`,
        subscription_discounts.invoice_id,
        subscription_discounts.invoice_item_id,
        subscription_discounts.promotion_code AS promotion_code_id,
        subscription_discounts.start
      )
      ORDER BY
        subscription_discounts.start DESC
      LIMIT
        1
    )[SAFE_ORDINAL(1)] AS discount
  FROM
    subscriptions_history
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.subscription_discount_v1 AS subscription_discounts
  ON
    subscriptions_history.subscription_id = subscription_discounts.subscription_id
    AND subscriptions_history._fivetran_start >= subscription_discounts.start
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.coupon_v1 AS coupons
  ON
    subscription_discounts.coupon_id = coupons.id
  GROUP BY
    subscriptions_history.id
)
SELECT
  subscriptions_history.id,
  subscriptions_history._fivetran_start AS `timestamp`,
  subscriptions_history._fivetran_synced AS synced_at,
  STRUCT(
    subscriptions_history.subscription_id AS id,
    subscriptions_history.billing_cycle_anchor,
    subscriptions_history.cancel_at,
    subscriptions_history.cancel_at_period_end,
    subscriptions_history.canceled_at,
    subscriptions_history.collection_method,
    subscriptions_history.created,
    subscriptions_history.current_period_end,
    subscriptions_history.current_period_start,
    subscriptions_history.customer_id,
    subscriptions_history.days_until_due,
    subscriptions_history.default_payment_method_id,
    subscriptions_history.default_source_id,
    COALESCE(subscriptions_history_tax_rates.tax_rates, []) AS default_tax_rates,
    subscriptions_history_latest_discounts.discount,
    subscriptions_history.ended_at,
    -- While SubPlat currently only allows subscriptions to have a single item,
    -- they're planning to allow subscriptions with multiple items at some point.
    [
      STRUCT(
        subscription_items.id,
        subscription_items.created,
        subscription_items.metadata,
        STRUCT(
          subscriptions_history.plan_id AS id,
          plans.aggregate_usage,
          plans.amount,
          plans.billing_scheme,
          plans.created,
          plans.currency,
          plans.`interval`,
          plans.interval_count,
          plans.metadata,
          plans.nickname,
          STRUCT(
            plans.product_id AS id,
            products.created,
            products.description,
            products.metadata,
            products.name,
            products.statement_descriptor,
            products.updated
          ) AS product,
          plans.tiers_mode,
          plans.trial_period_days,
          plans.usage_type
        ) AS plan,
        subscription_items.quantity
      )
    ] AS items,
    subscriptions_history.latest_invoice_id,
    subscriptions_history.metadata,
    subscriptions_history.pending_setup_intent_id,
    subscriptions_history.start_date,
    subscriptions_history.status,
    subscriptions_history.trial_end,
    subscriptions_history.trial_start
  ) AS subscription
FROM
  subscriptions_history_with_plan_ids AS subscriptions_history
JOIN
  subscription_items
ON
  subscriptions_history.subscription_id = subscription_items.subscription_id
LEFT JOIN
  plans
ON
  subscriptions_history.plan_id = plans.id
LEFT JOIN
  products
ON
  plans.product_id = products.id
LEFT JOIN
  subscriptions_history_tax_rates
ON
  subscriptions_history.id = subscriptions_history_tax_rates.subscription_history_id
LEFT JOIN
  subscriptions_history_latest_discounts
ON
  subscriptions_history.id = subscriptions_history_latest_discounts.subscription_history_id
WHERE
  {% if is_init() %}
    DATE(subscriptions_history._fivetran_start) < CURRENT_DATE()
  {% else %}
    DATE(subscriptions_history._fivetran_start) = @date
  {% endif %}
