SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  -- fields from the HLL-based core_client_count dataset
  app_name,
  metadata_app_version AS app_version,
  arch,
  normalized_channel,
  default_search,
  distribution_id,
  country,
  locale,
  os,
  osversion,
  COALESCE(cc.name, cls.country) AS country_name
FROM
  telemetry.core_clients_last_seen_v1 AS cls
LEFT JOIN
  static.country_codes_v1 AS cc
  ON (cls.country = cc.code)
WHERE
  client_id IS NOT NULL
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  app_name,
  app_version,
  arch,
  normalized_channel,
  default_search,
  distribution_id,
  country,
  locale,
  os,
  osversion,
  country_name
