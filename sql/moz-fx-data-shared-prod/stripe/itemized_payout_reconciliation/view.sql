CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.itemized_payout_reconciliation`
AS
WITH customers AS (
  SELECT
    id,
    address_country AS country,
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
  ON
    charges.card_id = cards.id
  JOIN
    postal_code_to_state
  ON
    cards.country = postal_code_to_state.country
    AND UPPER(TRIM(charges.billing_detail_address_postal_code)) = postal_code_to_state.postal_code
  WHERE
    cards.country IN ("US", "CA")
),
enriched AS (
  SELECT
    report.*,
    charge_states.state,
    charge_states.postal_code,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.itemized_payout_reconciliation_v5 AS report
  LEFT JOIN
    charge_states
  USING
    (charge_id, card_country)
)
SELECT
  * EXCEPT (card_country, state),
  CASE
    -- american samoa
    WHEN card_country = "US"
      AND REGEXP_CONTAINS(postal_code, "^96799(-?[0-9]{4})?$")
      THEN STRUCT("AS" AS card_country, NULL AS state)
    -- puerto rico
    WHEN card_country = "US"
      AND REGEXP_CONTAINS(postal_code, "^00[679][0-9]{2}(-?[0-9]{4})?$")
      THEN STRUCT("PR" AS card_country, NULL AS state)
    -- virgin islands
    WHEN card_country = "US"
      AND REGEXP_CONTAINS(postal_code, "^008[0-9]{2}(-?[0-9]{4})?$")
      THEN STRUCT("VI" AS card_country, NULL AS state)
    ELSE STRUCT(card_country, state)
  END.*,
FROM
  enriched
