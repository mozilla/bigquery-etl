CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.logical_subscriptions`
AS
WITH subscriptions AS (
  SELECT
    subscription.*,
    COALESCE(DATE(subscription.ended_at), CURRENT_DATE()) AS effective_date
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscriptions_history_v1`
  WHERE
    valid_to = '9999-12-31 23:59:59.999999'
),
vat_rates AS (
  SELECT
    country_code,
    vat AS vat_rate,
    effective_date,
    LEAD(effective_date) OVER (
      PARTITION BY
        country_code
      ORDER BY
        effective_date
    ) AS next_effective_date
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.vat_rates_v1`
),
usd_exchange_rates AS (
  SELECT
    base_currency,
    price AS exchange_rate,
    `date` AS effective_date,
    LEAD(`date`) OVER (PARTITION BY base_currency ORDER BY `date`) AS next_effective_date
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.exchange_rates_v1`
  WHERE
    quote_currency = 'USD'
),
augmented_subscriptions AS (
  SELECT
    subscriptions.*,
    vat_rates.vat_rate AS country_vat_rate,
    usd_exchange_rates.exchange_rate AS plan_currency_usd_exchange_rate,
  FROM
    subscriptions
  LEFT JOIN
    vat_rates
    ON subscriptions.country_code = vat_rates.country_code
    AND subscriptions.effective_date >= vat_rates.effective_date
    AND (
      subscriptions.effective_date < vat_rates.next_effective_date
      OR vat_rates.next_effective_date IS NULL
    )
  LEFT JOIN
    usd_exchange_rates
    ON subscriptions.plan_currency = usd_exchange_rates.base_currency
    AND subscriptions.effective_date >= usd_exchange_rates.effective_date
    AND (
      subscriptions.effective_date < usd_exchange_rates.next_effective_date
      OR usd_exchange_rates.next_effective_date IS NULL
    )
),
augmented_subscriptions_2 AS (
  SELECT
    *,
    LEAST(
      (
        mozfun.utils.timestamp_diff_complete_months(
          current_period_ends_at,
          LEAST(TIMESTAMP(CURRENT_DATE()), current_period_ends_at)
        ) + 1
      ),
      12
    ) AS current_period_annual_recurring_revenue_months,
    GREATEST(
      mozfun.utils.timestamp_diff_complete_months(
        TIMESTAMP_SUB(ongoing_discount_ends_at, INTERVAL 1 MICROSECOND),
        current_period_ends_at
      ),
      0
    ) AS months_after_current_period_before_ongoing_discount_ends
  FROM
    augmented_subscriptions
),
augmented_subscriptions_3 AS (
  SELECT
    *,
    CASE
      WHEN COALESCE(ongoing_discount_amount, 0) = 0
        THEN 0
      WHEN ongoing_discount_ends_at IS NULL
        THEN (12 - current_period_annual_recurring_revenue_months)
      ELSE LEAST(
          (
            (
              DIV(
                months_after_current_period_before_ongoing_discount_ends,
                plan_interval_months
              ) + 1
            ) * plan_interval_months
          ),
          (12 - current_period_annual_recurring_revenue_months)
        )
    END AS ongoing_discounted_annual_recurring_revenue_months
  FROM
    augmented_subscriptions_2
)
SELECT
  * EXCEPT (
    effective_date,
    current_period_annual_recurring_revenue_months,
    months_after_current_period_before_ongoing_discount_ends,
    ongoing_discounted_annual_recurring_revenue_months
  ),
  IF(
    plan_currency = 'USD',
    plan_amount,
    ROUND((plan_amount * plan_currency_usd_exchange_rate), 2)
  ) AS plan_amount_usd,
  IF(
    plan_currency = 'USD',
    current_period_discount_amount,
    ROUND((current_period_discount_amount * plan_currency_usd_exchange_rate), 2)
  ) AS current_period_discount_amount_usd,
  IF(
    plan_currency = 'USD',
    ongoing_discount_amount,
    ROUND((ongoing_discount_amount * plan_currency_usd_exchange_rate), 2)
  ) AS ongoing_discount_amount_usd,
  IF(
    is_active IS NOT TRUE
    OR is_trial IS TRUE,
    0,
    ROUND(
      (
        -- Start with monthly recurring gross revenue...
        (
          GREATEST(
            (plan_amount - COALESCE(current_period_discount_amount, 0)),
            0
          ) / plan_interval_months
        )
        -- Remove VAT to get monthly recurring net revenue.
        / (1 + COALESCE(country_vat_rate, 0))
        -- Apply exchange rate to get monthly recurring revenue in USD.
        * IF(plan_currency = 'USD', 1, plan_currency_usd_exchange_rate)
      ),
      2
    )
  ) AS monthly_recurring_revenue_usd,
  IF(
    is_active IS NOT TRUE,
    0,
    ROUND(
      (
        -- Start with annual recurring gross revenue...
        (
          -- Current period annual recurring gross revenue
          IF(
            is_trial IS TRUE,
            0,
            (
              GREATEST(
                (plan_amount - COALESCE(current_period_discount_amount, 0)),
                0
              ) / plan_interval_months * current_period_annual_recurring_revenue_months
            )
          )
          -- Ongoing discounted annual recurring gross revenue
          + IF(
            auto_renew IS NOT TRUE
            OR COALESCE(ongoing_discount_amount, 0) = 0,
            0,
            (
              GREATEST(
                (plan_amount - COALESCE(ongoing_discount_amount, 0)),
                0
              ) / plan_interval_months * ongoing_discounted_annual_recurring_revenue_months
            )
          )
          -- Ongoing undiscounted annual recurring gross revenue
          + IF(
            auto_renew IS NOT TRUE,
            0,
            (
              plan_amount / plan_interval_months * GREATEST(
                (
                  12 - current_period_annual_recurring_revenue_months - ongoing_discounted_annual_recurring_revenue_months
                ),
                0
              )
            )
          )
        )
        -- Remove VAT to get annual recurring net revenue.
        / (1 + COALESCE(country_vat_rate, 0))
        -- Apply exchange rate to get annual recurring revenue in USD.
        * IF(plan_currency = 'USD', 1, plan_currency_usd_exchange_rate)
      ),
      2
    )
  ) AS annual_recurring_revenue_usd
FROM
  augmented_subscriptions_3
