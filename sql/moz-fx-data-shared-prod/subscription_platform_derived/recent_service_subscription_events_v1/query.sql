SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.service_subscription_events_v1_live`
WHERE
  (
    DATE(`timestamp`) >= CURRENT_DATE() - 7
    -- Include more than the previous 7 days if necessary to avoid data gaps between this table and
    -- `service_subscription_events_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR DATE(`timestamp`) >= (
      SELECT
        MAX(DATE(`timestamp`)) + 1
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.service_subscription_events_v1`
    )
  )
  AND DATE(`timestamp`) <= CURRENT_DATE() - 1
