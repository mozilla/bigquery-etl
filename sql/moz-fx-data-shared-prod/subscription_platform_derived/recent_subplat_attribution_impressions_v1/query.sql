SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1_live`
WHERE
  (
    DATE(impression_at) >= CURRENT_DATE() - 1
    -- Include more than the previous day if necessary to avoid data gaps between this table and
    -- `subplat_attribution_impressions_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR DATE(impression_at) >= (
      SELECT
        MAX(DATE(impression_at)) + 1
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1`
    )
  )
  AND DATE(impression_at) <= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
