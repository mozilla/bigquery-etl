SELECT
  submission_date,
  release_channel,
  country,
  COALESCE(cc.name, ouls.country) AS country_name,
  locale,
  version,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
FROM
  messaging_system.onboarding_users_last_seen AS ouls
LEFT JOIN
  static.country_codes_v1 AS cc
  ON (ouls.country = cc.code)
WHERE
  client_id IS NOT NULL
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  release_channel,
  country,
  country_name,
  locale,
  version
