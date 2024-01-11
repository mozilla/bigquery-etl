CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.itemized_payout_reconciliation`
AS
WITH customers AS (
  SELECT
    id AS customer_id,
    NULLIF(address_country, "") AS country,
    NULLIF(UPPER(TRIM(address_postal_code)), "") AS postal_code,
    NULLIF(address_state, "") AS state,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.customer_v1
),
postal_code_to_state AS (
  SELECT
    country,
    postal_code,
    IF(COUNT(DISTINCT state) > 1, NULL, ANY_VALUE(state)) AS state,
  FROM
    customers
  WHERE
    country IN ("US", "CA")
    AND postal_code IS NOT NULL
    AND state IS NOT NULL
    AND LENGTH(state) = 2
  GROUP BY
    country,
    postal_code
),
charge_states AS (
  SELECT
    charges.id AS charge_id,
    cards.country AS card_country,
    NULLIF(UPPER(TRIM(charges.billing_detail_address_postal_code)), "") AS postal_code,
    postal_code_to_state.state,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.charge_v1 AS charges
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.card_v1 AS cards
    ON charges.card_id = cards.id
  JOIN
    postal_code_to_state
    ON cards.country = postal_code_to_state.country
    AND UPPER(TRIM(charges.billing_detail_address_postal_code)) = postal_code_to_state.postal_code
  WHERE
    cards.country IN ("US", "CA")
),
subscriptions AS (
  SELECT
    subscription_id,
    plan_id,
    plan_name,
    plan_interval,
    plan_interval_count,
    product_id,
    product_name,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform.stripe_subscriptions
),
taxes_by_address AS (
  SELECT
    id AS invoice_id,
    NULLIF(destination_resolved_address_country, "") AS country,
    NULLIF(destination_resolved_address_state, "") AS state,
    NULLIF(UPPER(TRIM(destination_resolved_address_postal_code)), "") AS postal_code,
    currency,
    SUM(tax_amount) AS tax_amount,
    ROW_NUMBER() OVER (PARTITION BY id) AS row_number,
  FROM
    `moz-fx-data-shared-prod`.stripe.itemized_tax_transactions
  GROUP BY
    invoice_id,
    country,
    state,
    postal_code,
    currency
),
taxes AS (
  SELECT
    invoice_id,
    -- There should only be one set of distinct values for these fields, but if there are more then
    -- they will be CSVs with the same length and order for consistency
    STRING_AGG(country, ", " ORDER BY row_number) AS tax_country,
    STRING_AGG(state, ", " ORDER BY row_number) AS tax_state,
    STRING_AGG(postal_code, ", " ORDER BY row_number) AS tax_postal_code,
    STRING_AGG(currency, ", " ORDER BY row_number) AS tax_currency,
    SUM(tax_amount) AS tax_amount,
  FROM
    taxes_by_address
  GROUP BY
    invoice_id
)
SELECT
  report.* EXCEPT (card_country),
  -- This logic preserves legacy behavior for card address fields, but they are no longer in use
  -- and have been superceded by tax_{country,state,postal_code} fields. The new fields do not
  -- indicate US territories the same way, and are not compatible with this logic.
  CASE
    -- American Samoa
    WHEN report.card_country = "US"
      AND REGEXP_CONTAINS(charge_states.postal_code, "^96799(-?[0-9]{4})?$")
      THEN STRUCT("AS" AS card_country, NULL AS card_state)
    -- Puerto Rico
    WHEN report.card_country = "US"
      AND REGEXP_CONTAINS(charge_states.postal_code, "^00[679][0-9]{2}(-?[0-9]{4})?$")
      THEN STRUCT("PR" AS card_country, NULL AS card_state)
    -- Virgin Islands
    WHEN report.card_country = "US"
      AND REGEXP_CONTAINS(charge_states.postal_code, "^008[0-9]{2}(-?[0-9]{4})?$")
      THEN STRUCT("VI" AS card_country, NULL AS card_state)
    ELSE STRUCT(report.card_country, charge_states.state AS card_state)
  END.*,
  charge_states.postal_code AS card_postal_code,
  subscriptions.* EXCEPT (subscription_id),
  taxes.* EXCEPT (invoice_id),
FROM
  `moz-fx-data-shared-prod`.stripe_external.itemized_payout_reconciliation_v5 AS report
LEFT JOIN
  charge_states
  USING (charge_id, card_country)
LEFT JOIN
  subscriptions
  USING (subscription_id)
LEFT JOIN
  taxes
  USING (invoice_id)
