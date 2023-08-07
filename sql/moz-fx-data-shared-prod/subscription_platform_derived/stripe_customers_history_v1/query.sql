SELECT
  id,
  `timestamp` AS valid_from,
  COALESCE(
    LEAD(`timestamp`) OVER (PARTITION BY customer.id ORDER BY `timestamp`),
    '9999-12-31 23:59:59.999999'
  ) AS valid_to,
  id AS stripe_customers_revised_changelog_id,
  customer
FROM
  `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_customers_revised_changelog_v1
