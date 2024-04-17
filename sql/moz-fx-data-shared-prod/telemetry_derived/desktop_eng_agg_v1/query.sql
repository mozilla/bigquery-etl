-- Query for telemetry_derived.desktop_eng_agg_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  submission_date,
  first_seen_date,
  attribution_campaign,
  attribution_content,
  attribution_dlsource,
  attribution_medium,
  attribution_ua,
  normalized_os,
  normalized_os_version,
  country,
  lifecycle_stage,
  sum(CASE WHEN dau THEN 1 ELSE 0 END) AS dau,
  sum(CASE WHEN wau THEN 1 ELSE 0 END) AS wau,
  sum(CASE WHEN mau THEN 1 ELSE 0 END) AS mau
FROM
  `moz-fx-data-shared-prod.telemetry.desktop_eng_client`
WHERE
  submission_date = @submission_date
GROUP BY 
  submission_date,
  first_seen_date,
  attribution_campaign,
  attribution_content,
  attribution_dlsource,
  aatribution_medium,
  attribution_ua,
  normalized_os,
  normalized_os_version,
  country,
  lifecycle_stage