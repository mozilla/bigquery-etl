CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.logical_subscription_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscription_events_v1`
WHERE
  DATE(`timestamp`) < (
    SELECT
      COALESCE(MIN(DATE(`timestamp`)), '9999-12-31')
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.recent_logical_subscription_events_v1`
  )
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.recent_logical_subscription_events_v1`
