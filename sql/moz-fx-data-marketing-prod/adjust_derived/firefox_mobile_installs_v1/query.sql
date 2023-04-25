SELECT
  date,
  app,
  network,
  campaign,
  adgroup,
  creative,
  UPPER(adj.country) AS country,
  ctry.name AS country_name,
  os,
  device_type,
  SUM(clicks) AS clicks,
  SUM(installs) AS installs
FROM
  `ga-mozilla-org-prod-001.Adjust.deliverable_*` adj
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` ctry
ON
  UPPER(adj.country) = ctry.country
WHERE
  app = 'Firefox Android and iOS'
  AND date >= '2022-01-01'
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10
