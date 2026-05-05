SELECT
  id,
  created,
  updated,
  is_deleted,
  active,
  description,
  PARSE_JSON(metadata) AS metadata,
  name,
  statement_descriptor,
FROM
  `moz-fx-data-shared-prod.stripe_external.product_v1`
