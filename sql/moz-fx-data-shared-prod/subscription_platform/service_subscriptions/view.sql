CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.service_subscriptions`
AS
WITH subscriptions AS (
  SELECT
    *,
    COALESCE(DATE(ended_at), CURRENT_DATE()) AS effective_date
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.service_subscriptions_v1`
),
augmented_subscriptions AS (
  SELECT
    subscriptions.*,
    vat_rates.vat_rate AS country_vat_rate,
    usd_exchange_rates.exchange_rate AS plan_currency_usd_exchange_rate,
  FROM
    subscriptions
  LEFT JOIN
    `moz-fx-data-shared-prod.subscription_platform.vat_rates_history` AS vat_rates
    ON subscriptions.country_code = vat_rates.country_code
    AND (subscriptions.effective_date BETWEEN vat_rates.valid_from AND vat_rates.valid_to)
  LEFT JOIN
    `moz-fx-data-shared-prod.subscription_platform.exchange_rates_history` AS usd_exchange_rates
    ON subscriptions.plan_currency = usd_exchange_rates.base_currency
    AND usd_exchange_rates.quote_currency = 'USD'
    AND (
      subscriptions.effective_date
      BETWEEN usd_exchange_rates.valid_from
      AND usd_exchange_rates.valid_to
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
