-- raw SQL checks
-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
-- Retrieves the maximum subscription updated timestamp from the last run to only
-- select recently changed records
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.products_v1`,
    UNNEST(products) AS products
  WHERE
    products.subscription_updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 HOUR)
) > 0
AS
  "No new records in the braze_derived.products_v1 table in the last 15 hours";

-- macro checks

#fail
{{ not_null(["external_id"]) }} -- to do: add array values

#fail
{{ is_unique(["external_id"]) }}

#fail
{{ min_row_count(1) }}
