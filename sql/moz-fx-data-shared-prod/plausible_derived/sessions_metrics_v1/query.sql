-- Query for plausible_derived.sessions_metrics_v1
-- Daily rollup of Plausible sessions by their entry page and country.
-- Single scan of sessions_v1, no joins.
SELECT
  @submission_date AS `date`,
  entry_page,
  country_code,
  acquisition_channel,
  utm_campaign,
  COUNT(*) AS entrances,
  COUNTIF(is_bounce) AS bounces
FROM
  `moz-fx-data-shared-prod.plausible_external.sessions_v1`
WHERE
  DATE(start) = @submission_date
GROUP BY
  `date`,
  entry_page,
  country_code,
  acquisition_channel,
  utm_campaign
