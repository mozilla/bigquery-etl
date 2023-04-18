SELECT
  date,
  app,
  network,
  campaign,
  adgroup,
  creative,
  CASE
  WHEN
    country = 'us'
  THEN
    'United States'
  WHEN
    country = 'de'
  THEN
    'Germany'
  WHEN
    country = 'it'
  THEN
    'Italy'
  WHEN
    country = 'uk'
  THEN
    'United Kingdom'
  WHEN
    country = 'ca'
  THEN
    'Canada'
  WHEN
    country = 'es'
  THEN
    'Spain'
  WHEN
    country = 'pl'
  THEN
    'Poland'
  WHEN
    country = 'fr'
  THEN
    'France'
  ELSE
    'Other'
  END
  AS country,
  os,
  device_type,
  sum(clicks) AS clicks,
  SUM(installs) AS installs
FROM
  `ga-mozilla-org-prod-001.Adjust.deliverable_*`
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
  9
