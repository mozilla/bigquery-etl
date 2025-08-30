CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.monthly_active_logical_subscriptions`
AS
WITH monthly_subscriptions AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1`
  WHERE
    month_start_date < (
      SELECT
        COALESCE(MIN(month_start_date), '9999-12-31')
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_logical_subscriptions_v1`
    )
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_logical_subscriptions_v1`
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
augmented_monthly_subscriptions AS (
  SELECT
    *,
    COALESCE(DATE(subscription.ended_at), LEAST(month_end_date, CURRENT_DATE())) AS effective_date
  FROM
    monthly_subscriptions
),
augmented_monthly_subscriptions_2 AS (
  SELECT
    monthly_subscriptions.*,
    vat_rates.vat_rate AS country_vat_rate,
    usd_exchange_rates.exchange_rate AS plan_currency_usd_exchange_rate,
  FROM
    augmented_monthly_subscriptions AS monthly_subscriptions
  LEFT JOIN
    vat_rates
    ON monthly_subscriptions.subscription.country_code = vat_rates.country_code
    AND monthly_subscriptions.effective_date >= vat_rates.effective_date
    AND (
      monthly_subscriptions.effective_date < vat_rates.next_effective_date
      OR vat_rates.next_effective_date IS NULL
    )
  LEFT JOIN
    usd_exchange_rates
    ON monthly_subscriptions.subscription.plan_currency = usd_exchange_rates.base_currency
    AND monthly_subscriptions.effective_date >= usd_exchange_rates.effective_date
    AND (
      monthly_subscriptions.effective_date < usd_exchange_rates.next_effective_date
      OR usd_exchange_rates.next_effective_date IS NULL
    )
),
augmented_monthly_subscriptions_3 AS (
  SELECT
    *,
    LEAST(
      (
        mozfun.utils.timestamp_diff_complete_months(
          subscription.current_period_ends_at,
          LEAST(
            TIMESTAMP_SUB(TIMESTAMP(month_end_date + 1), INTERVAL 1 MICROSECOND),
            TIMESTAMP(CURRENT_DATE()),
            subscription.current_period_ends_at
          )
        ) + 1
      ),
      12
    ) AS current_period_annual_recurring_revenue_months,
    GREATEST(
      mozfun.utils.timestamp_diff_complete_months(
        TIMESTAMP_SUB(subscription.ongoing_discount_ends_at, INTERVAL 1 MICROSECOND),
        subscription.current_period_ends_at
      ),
      0
    ) AS months_after_current_period_before_ongoing_discount_ends
  FROM
    augmented_monthly_subscriptions_2
),
augmented_monthly_subscriptions_4 AS (
  SELECT
    *,
    CASE
      WHEN COALESCE(subscription.ongoing_discount_amount, 0) = 0
        THEN 0
      WHEN subscription.ongoing_discount_ends_at IS NULL
        THEN (12 - current_period_annual_recurring_revenue_months)
      ELSE LEAST(
          (
            (
              DIV(
                months_after_current_period_before_ongoing_discount_ends,
                subscription.plan_interval_months
              ) + 1
            ) * subscription.plan_interval_months
          ),
          (12 - current_period_annual_recurring_revenue_months)
        )
    END AS ongoing_discounted_annual_recurring_revenue_months
  FROM
    augmented_monthly_subscriptions_3
)
SELECT
  id,
  month_start_date,
  month_end_date,
  logical_subscriptions_history_id,
  (
    SELECT AS STRUCT
      subscription.*,
      country_vat_rate,
      plan_currency_usd_exchange_rate,
      IF(
        subscription.plan_currency = 'USD',
        subscription.plan_amount,
        ROUND((subscription.plan_amount * plan_currency_usd_exchange_rate), 2)
      ) AS plan_amount_usd,
      IF(
        subscription.plan_currency = 'USD',
        subscription.current_period_discount_amount,
        ROUND((subscription.current_period_discount_amount * plan_currency_usd_exchange_rate), 2)
      ) AS current_period_discount_amount_usd,
      IF(
        subscription.plan_currency = 'USD',
        subscription.ongoing_discount_amount,
        ROUND((subscription.ongoing_discount_amount * plan_currency_usd_exchange_rate), 2)
      ) AS ongoing_discount_amount_usd,
      IF(
        subscription.is_active IS NOT TRUE
        OR subscription.is_trial IS TRUE,
        0,
        ROUND(
          (
            -- Start with monthly recurring gross revenue...
            (
              GREATEST(
                (
                  subscription.plan_amount - COALESCE(
                    subscription.current_period_discount_amount,
                    0
                  )
                ),
                0
              ) / subscription.plan_interval_months
            )
            -- Remove VAT to get monthly recurring net revenue.
            / (1 + COALESCE(country_vat_rate, 0))
            -- Apply exchange rate to get monthly recurring revenue in USD.
            * IF(subscription.plan_currency = 'USD', 1, plan_currency_usd_exchange_rate)
          ),
          2
        )
      ) AS monthly_recurring_revenue_usd,
      IF(
        subscription.is_active IS NOT TRUE,
        0,
        ROUND(
          (
            -- Start with annual recurring gross revenue...
            (
              -- Current period annual recurring gross revenue
              IF(
                subscription.is_trial IS TRUE,
                0,
                (
                  GREATEST(
                    (
                      subscription.plan_amount - COALESCE(
                        subscription.current_period_discount_amount,
                        0
                      )
                    ),
                    0
                  ) / subscription.plan_interval_months * current_period_annual_recurring_revenue_months
                )
              )
              -- Ongoing discounted annual recurring gross revenue
              + IF(
                subscription.auto_renew IS NOT TRUE
                OR COALESCE(subscription.ongoing_discount_amount, 0) = 0,
                0,
                (
                  GREATEST(
                    (subscription.plan_amount - COALESCE(subscription.ongoing_discount_amount, 0)),
                    0
                  ) / subscription.plan_interval_months * ongoing_discounted_annual_recurring_revenue_months
                )
              )
              -- Ongoing undiscounted annual recurring gross revenue
              + IF(
                subscription.auto_renew IS NOT TRUE,
                0,
                (
                  subscription.plan_amount / subscription.plan_interval_months * GREATEST(
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
            * IF(subscription.plan_currency = 'USD', 1, plan_currency_usd_exchange_rate)
          ),
          2
        )
      ) AS annual_recurring_revenue_usd
  ) AS subscription,
  was_active_at_month_start,
  was_active_at_month_end
FROM
  augmented_monthly_subscriptions_4
