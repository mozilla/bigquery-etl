SELECT
  submission_date,
  release_channel,
  country,
  COALESCE(cc.name, culs.country) AS country_name,
  locale,
  version,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  COUNTIF(days_since_seen_whats_new < 28) AS whats_new_mau,
  COUNTIF(days_since_seen_whats_new < 7) AS whats_new_wau,
  COUNTIF(days_since_seen_whats_new < 1) AS whats_new_dau
FROM
  messaging_system.cfr_users_last_seen AS culs
LEFT JOIN
  static.country_codes_v1 AS cc
  ON (culs.country = cc.code)
WHERE
  (client_id IS NOT NULL OR impression_id IS NOT NULL)
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  release_channel,
  country,
  country_name,
  locale,
  version
