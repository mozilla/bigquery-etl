WITH flows_live AS (
  SELECT
    *
  FROM
    `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_v1
  WHERE
    submission_date < CURRENT_DATE
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_live
  WHERE
    submission_date = CURRENT_DATE
),
aic_flows AS (
  -- last fxa_uid for each flow_id
  SELECT
    flow_id,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(fxa_uid IGNORE NULLS ORDER BY fxa_uid_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS fxa_uid,
  FROM
    flows_live
  JOIN
    EXTERNAL_QUERY("moz-fx-cjms-nonprod-9a36.us.cjms-sql", "SELECT flow_id FROM aic")
  USING
    (flow_id)
  WHERE
    -- only use the last 10 days in stage
    submission_date >= CURRENT_DATE - 10
  GROUP BY
    flow_id
),
attributed_flows AS (
  -- last flow that started before subscription created
  SELECT
    subscriptions.subscription_id,
    subscriptions.created AS subscription_created,
    ARRAY_AGG(aic_flows.flow_id ORDER BY aic_flows.flow_started DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS flow_id,
  FROM
    aic_flows
  JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.nonprod_stripe_subscriptions_v1 AS subscriptions
  ON
    aic_flows.fxa_uid = subscriptions.fxa_uid
    AND aic_flows.flow_started < subscriptions.created
  GROUP BY
    subscription_id,
    subscription_created
),
attributed_subs AS (
  -- first subscription for each flow, for 1:1 relationship between flow and subscription
  SELECT
    flow_id,
    ARRAY_AGG(subscription_id ORDER BY subscription_created LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS subscription_id,
  FROM
    attributed_flows
  GROUP BY
    flow_id
),
initial_invoices AS (
  SELECT
    subscription_id,
    invoices.id AS invoice_id,
  FROM
    attributed_subs
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.nonprod_invoice_v1 AS invoices
  USING
    (subscription_id)
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY invoices.created)
),
initial_discounts AS (
  SELECT
    initial_invoices.subscription_id,
    promotion_codes.code AS promotion_code,
    coupons.amount_off,
    coupons.percent_off,
  FROM
    initial_invoices
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.nonprod_invoice_discount_v1 AS invoice_discounts
  USING
    (invoice_id)
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.nonprod_promotion_code_v1 AS promotion_codes
  ON
    invoice_discounts.promotion_code = promotion_codes.id
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.nonprod_coupon_v1 AS coupons
  ON
    promotion_codes.coupon_id = coupons.id
),
promotion_codes AS (
  SELECT
    subscription_id,
    STRING_AGG(promotion_code, " " ORDER BY promotion_code) AS promotion_codes,
  FROM
    initial_discounts
  GROUP BY
    subscription_id
),
amount_discounts AS (
  SELECT
    subscription_id,
    SUM(amount_off) AS amount_off,
  FROM
    initial_discounts
  WHERE
    amount_off IS NOT NULL
  GROUP BY
    subscription_id
),
percent_discounts AS (
  SELECT
    subscription_id,
    subscriptions.subscription_item_id,
    SUM(CAST((subscriptions.plan_amount * (discounts.percent_off / 100)) AS INT64)) AS amount_off,
  FROM
    initial_discounts AS discounts
  JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.nonprod_stripe_subscriptions_v1 AS subscriptions
  USING
    (subscription_id)
  WHERE
    discounts.percent_off IS NOT NULL
  GROUP BY
    subscription_id,
    subscription_item_id
)
SELECT
  CURRENT_TIMESTAMP AS report_timestamp,
  subscriptions.created AS subscription_created,
  attributed_subs.subscription_id, -- transaction id
  subscriptions.fxa_uid,
  1 AS quantity,
  subscriptions.plan_id, -- sku
  subscriptions.plan_currency,
  (
    subscriptions.plan_amount - COALESCE(amount_discounts.amount_off, 0) - COALESCE(
      percent_discounts.amount_off,
      0
    )
  ) AS plan_amount,
  subscriptions.country,
  attributed_subs.flow_id,
  promotion_codes.promotion_codes,
FROM
  attributed_subs
JOIN
  `moz-fx-data-shared-prod`.subscription_platform_derived.nonprod_stripe_subscriptions_v1 AS subscriptions
USING
  (subscription_id)
LEFT JOIN
  promotion_codes
USING
  (subscription_id)
LEFT JOIN
  amount_discounts
USING
  (subscription_id)
LEFT JOIN
  percent_discounts
USING
  (subscription_id, subscription_item_id)
