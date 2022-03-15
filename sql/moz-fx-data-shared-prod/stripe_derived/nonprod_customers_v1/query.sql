SELECT
  -- limit fields in stripe_derived so as not to expose sensitive data
  created,
  id,
  metadata,
FROM
  stripe_external.nonprod_customers_v1
