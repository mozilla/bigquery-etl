SELECT
  id,
  product_id,
  created,
  is_deleted,
  active,
  aggregate_usage,
  amount,
  billing_scheme,
  currency,
  `interval`,
  interval_count,
  PARSE_JSON(metadata) AS metadata,
  nickname,
  tiers_mode,
  trial_period_days,
  usage_type,
  ARRAY(
    SELECT
      TRIM(apple_product_id)
    FROM
      UNNEST(SPLIT(JSON_VALUE(metadata, '$.appStoreProductIds'), ',')) AS apple_product_id
    WHERE
      NULLIF(TRIM(apple_product_id), '') IS NOT NULL
  ) AS apple_product_ids,
  ARRAY(
    SELECT
      TRIM(google_sku_id)
    FROM
      UNNEST(SPLIT(JSON_VALUE(metadata, '$.playSkuIds'), ',')) AS google_sku_id
    WHERE
      NULLIF(TRIM(google_sku_id), '') IS NOT NULL
  ) AS google_sku_ids,
FROM
  `moz-fx-data-shared-prod.stripe_external.plan_v1`
