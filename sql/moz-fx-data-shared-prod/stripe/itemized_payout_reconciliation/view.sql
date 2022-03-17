CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.itemized_payout_reconciliation`
AS
WITH customer AS (
  SELECT
    id,
    address_country AS country,
    NULLIF(UPPER(TRIM(address_postal_code)), "") AS postal_code,
    NULLIF(address_state, "") AS state,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.customer
),
postal_code_to_state AS (
  SELECT
    country,
    postal_code,
    IF(COUNT(DISTINCT state) > 1, NULL, ANY_VALUE(state)) AS state,
  FROM
    customer
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
    charge.id AS charge_id,
    card.country AS card_country,
    NULLIF(UPPER(TRIM(charge.billing_detail_address_postal_code)), "") AS postal_code,
    postal_code_to_state.state,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.charge
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.card
  ON
    charge.source_id = card.id
  JOIN
    postal_code_to_state
  ON
    card.country = postal_code_to_state.country
    AND UPPER(TRIM(charge.billing_detail_address_postal_code)) = postal_code_to_state.postal_code
  WHERE
    card.country IN ("US", "CA")
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
  WHEN
    card_country = "US"
    AND REGEXP_CONTAINS(postal_code, "^96799(-?[0-9]{4})?$")
  THEN
    STRUCT("AS" AS card_country, NULL AS state)
  -- puerto rico
  WHEN
    card_country = "US"
    AND REGEXP_CONTAINS(postal_code, "^00[679][0-9]{2}(-?[0-9]{4})?$")
  THEN
    STRUCT("PR" AS card_country, NULL AS state)
  -- virgin islands
  WHEN
    card_country = "US"
    AND REGEXP_CONTAINS(postal_code, "^008[0-9]{2}(-?[0-9]{4})?$")
  THEN
    STRUCT("VI" AS card_country, NULL AS state)
  ELSE
    STRUCT(card_country, state)
  END
  .*,
FROM
  enriched
