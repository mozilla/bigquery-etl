SELECT
  submission_date,
  first_seen_date,
  attribution_campaign,
  attribution_content,
  attribution_dlsource,
  attribution_medium,
  attribution_ua,
  normalized_os,
  os_version_major,
  country,
  lifecycle_stage,
  SUM(CASE WHEN dau THEN 1 ELSE 0 END) AS dau,
  SUM(CASE WHEN wau THEN 1 ELSE 0 END) AS wau,
  SUM(CASE WHEN mau THEN 1 ELSE 0 END) AS mau
FROM
  `moz-fx-data-shared-prod.telemetry.desktop_engagement_client`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  first_seen_date,
  attribution_campaign,
  attribution_content,
  attribution_dlsource,
  attribution_medium,
  attribution_ua,
  normalized_os,
  os_version_major,
  country,
  lifecycle_stage
