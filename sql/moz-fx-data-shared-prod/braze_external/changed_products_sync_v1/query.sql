WITH max_update AS (
  SELECT
    MAX(UPDATED_AT) AS max_update_timestamp
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_products_sync_v1`
  LIMIT
    1
)
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  products.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          products_array.plan_id AS plan_id,
          products_array.product_id AS product_id,
          products_array.status AS status,
          products_array.plan_started_at AS plan_started_at,
          products_array.plan_ended_at AS plan_ended_at,
          products_array.plan_interval AS plan_interval,
          products_array.update_timestamp AS update_timestamp
        )
        ORDER BY
          products_array.update_timestamp DESC
      ) AS products
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.products_v1` AS products
CROSS JOIN
  UNNEST(products.products) AS products_array
WHERE
  products_array.update_timestamp > (SELECT max_update_timestamp FROM max_update)
GROUP BY
  products.external_id;
