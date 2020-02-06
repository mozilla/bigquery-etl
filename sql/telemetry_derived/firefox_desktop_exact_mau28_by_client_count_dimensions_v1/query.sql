SELECT
  submission_date,
  COUNTIF(days_since_seen < 28) AS mau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 1) AS dau,
  -- Active MAU counts all Active Users on any day in the last 28 days not just
  -- the most recent day making COUNTIF(_days_since_seen < 28 AND visited_5_uri)
  -- incorrect. Instead we track days_since_visited_5_uri and use that.
  -- https://docs.telemetry.mozilla.org/cookbooks/active_dau.html
  COUNTIF(days_since_visited_5_uri < 28) AS visited_5_uri_mau,
  COUNTIF(days_since_visited_5_uri < 7) AS visited_5_uri_wau,
  COUNTIF(days_since_visited_5_uri < 1) AS visited_5_uri_dau,
  -- dev_tools_mau counts users who opened dev tools in the last 28 days
  COUNTIF(days_since_opened_dev_tools < 28) AS dev_tools_mau,
  COUNTIF(days_since_opened_dev_tools < 7) AS dev_tools_wau,
  COUNTIF(days_since_opened_dev_tools < 1) AS dev_tools_dau,
  -- fields from the HLL-based client_count dataset
  app_name,
  app_version,
  country,
  locale,
  normalized_channel,
  os,
  os_version,
  distribution_id,
  COALESCE(cc.name, cls.country) AS country_name
FROM
  telemetry.clients_last_seen AS cls
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
  country,
  locale,
  normalized_channel,
  os,
  os_version,
  distribution_id,
  country_name
