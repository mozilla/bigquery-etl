-- Query for telemetry_derived.mobile_engagement_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  submission_date,
  first_seen_date,
  locale,
  app_version,
  normalized_os,
  normalized_os_version,
  country,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau
FROM
  `moz-fx-data-shared-prod.telemetry_derived.mobile_engagement_clients_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  first_seen_date,
  locale,
  app_version,
  normalized_os,
  normalized_os_version,
  country
