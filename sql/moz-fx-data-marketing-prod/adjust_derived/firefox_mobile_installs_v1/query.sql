SELECT
  adjust.date,
  adjust.network,
  adjust.campaign,
  adjust.adgroup,
  adjust.creative,
  UPPER(adjust.country) AS country,
  country.name AS country_name,
  adjust.os,
  adjust.device_type,
  SUM(adjust.clicks) AS clicks,
  SUM(adjust.installs) AS installs
FROM
  `ga-mozilla-org-prod-001.Adjust.deliverable_*` adjust
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` country
ON
  UPPER(adjust.country) = country.country
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
