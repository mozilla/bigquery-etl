-- raw SQL checks
-- checking to make sure there is at least one UPDATED_AT value

#fail
ASSERT(
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_products_sync_v1`
  WHERE
    UPDATED_AT IS NOT NULL
) > 0;

-- checking to see if there is new data since the last run
-- if not, fail or we will have blank sync tables

#fail
ASSERT(
  WITH max_update AS (
    SELECT
      MAX(UPDATED_AT) AS max_updated_at
    FROM
      `moz-fx-data-shared-prod.braze_external.changed_products_sync_v1`
  )
  SELECT
    COUNT(1)
  FROM
    `moz-fx-data-shared-prod.braze_derived.products_v1`,
    UNNEST(products) AS products,
    max_update
  WHERE
    products.subscription_updated_at > max_update.max_updated_at
) > 0;
