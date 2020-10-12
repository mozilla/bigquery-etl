SELECT
  ARRAY(
    SELECT
      platform
    FROM
      UNNEST(SPLIT(platforms, ",")) AS platform
    WHERE
      -- don't expose invalid platform values
      platform IN ("windows", "mac", "linux", "android", "ios", "chromebook")
  ) AS platforms,
  country,
  region,
  region_subdivision,
  DATE(created_at) AS waitlist_date,
  DATE(joined_date) AS joined_date,
FROM
  mozilla_vpn_external.waitlist_v1
