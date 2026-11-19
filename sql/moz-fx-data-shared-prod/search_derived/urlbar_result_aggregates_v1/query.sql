-- Desktop urlbar results per day, with client_id removed: one row per dimension set, prefs,
-- bot flag and result. Dimensions come from desktop_client_day_dims_v1.
WITH urlbar AS (
  SELECT
    client_id,
    pref_suggest_enabled,
    pref_sponsored_enabled,
    pref_online_suggest_available,
    pref_online_suggest_enabled,
    pref_data_collection,
    is_bot,
    results
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1`
  WHERE
    submission_date = @submission_date
),
-- urlbar_clients_daily_v1 has no date_partition_offset, so the urlbar task this run waits on
-- writes the next day. The partition read here comes from the previous run. If that run
-- didn't land, fail rather than write an empty day.
urlbar_partition_check AS (
  SELECT
    IF(
      COUNT(*) = 0,
      ERROR(FORMAT('urlbar_clients_daily_v1 has no rows for %t', @submission_date)),
      TRUE
    ) AS ok
  FROM
    urlbar
),
dims AS (
  SELECT
    client_id,
    country,
    normalized_channel,
    os,
    app_version_major,
    normalized_default_search_engine,
    is_default_browser
  FROM
    `moz-fx-data-shared-prod.search_derived.desktop_client_day_dims_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  @submission_date AS submission_date,
  d.country,
  d.normalized_channel,
  d.os,
  d.app_version_major,
  d.normalized_default_search_engine,
  d.is_default_browser,
  u.pref_suggest_enabled,
  u.pref_sponsored_enabled,
  u.pref_online_suggest_available,
  u.pref_online_suggest_enabled,
  u.pref_data_collection,
  u.is_bot,
  r.window_mode,
  r.sap,
  r.interaction,
  r.result_type,
  -- One value per result_type today, so grouping on it adds no rows. A key rather than
  -- ANY_VALUE, so a type that ever maps to two product names keeps both.
  r.product_result_type,
  SUM(r.clicks) AS clicks,
  SUM(r.sessions_shown) AS sessions_shown,
  SUM(r.sessions_shown_abandoned) AS sessions_shown_abandoned,
  SUM(r.annoyances) AS annoyances,
  COUNT(DISTINCT u.client_id) AS client_count
FROM
  urlbar AS u
CROSS JOIN
  UNNEST(u.results) AS r
LEFT JOIN
  dims AS d
  USING (client_id)
CROSS JOIN
  urlbar_partition_check
WHERE
  urlbar_partition_check.ok
GROUP BY
  d.country,
  d.normalized_channel,
  d.os,
  d.app_version_major,
  d.normalized_default_search_engine,
  d.is_default_browser,
  u.pref_suggest_enabled,
  u.pref_sponsored_enabled,
  u.pref_online_suggest_available,
  u.pref_online_suggest_enabled,
  u.pref_data_collection,
  u.is_bot,
  r.window_mode,
  r.sap,
  r.interaction,
  r.result_type,
  r.product_result_type
