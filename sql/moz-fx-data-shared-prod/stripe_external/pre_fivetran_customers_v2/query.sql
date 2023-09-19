SELECT
  id,
  STRUCT(
    address_country AS country,
    address_postal_code AS postal_code,
    address_state AS state
  ) AS address,
  created,
  is_deleted,
  PARSE_JSON(REPLACE(metadata, '"fxa_uid"', '"userid_sha256"')) AS metadata
FROM
  `moz-fx-data-shared-prod`.stripe_external.pre_fivetran_customers_v1
