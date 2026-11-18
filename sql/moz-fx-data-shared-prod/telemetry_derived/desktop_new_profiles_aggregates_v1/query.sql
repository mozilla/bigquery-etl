WITH clients_first_seen AS (
  SELECT
    client_id,
    first_seen_date,
    is_desktop,
    attribution_medium,
    attribution_source,
    attribution_campaign,
    attribution_content,
    attribution_dlsource,
    attribution_ua,
    (attribution_medium IS NOT NULL OR attribution_source IS NOT NULL) AS attributed,
    `moz-fx-data-shared-prod.udf.organic_vs_paid_desktop`(attribution_medium) AS paid_vs_organic,
    city,
    country,
    distribution_id,
    normalized_channel AS channel,
    os,
    os_version,
    normalized_os,
    normalized_os_version,
    locale,
    app_version,
    windows_version,
    windows_build_number
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen` cfs
  WHERE
    cfs.first_seen_date = @submission_date
    -- This table feeds dashboards directly, so confirmed automated (bot) clients are excluded
    -- outright rather than flagged. They come from telemetry_derived.desktop_automated_clients_v1
    -- (DENG-11925), which includes the DENG-11590 segment. Exclude every fleet. Only the
    -- DENG-11590 subset is flagged on telemetry.desktop_new_profiles_clients.
    AND cfs.client_id NOT IN (
      -- Only flags up to @submission_date, so a rerun or backfill gives the same result as the
      -- daily run. The table is read directly because a membership test needs no per-client
      -- grouping. NOT IN is NULL for every row if the subquery returns a NULL, so drop NULLs.
      SELECT
        client_id
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.desktop_automated_clients_v1`
      WHERE
        submission_date <= @submission_date
        AND client_id IS NOT NULL
    )
),
active_users AS (
  SELECT
    client_id,
    is_dau,
    submission_date
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users` au
  WHERE
    au.submission_date = @submission_date
)
SELECT
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  cfs.attributed,
  cfs.paid_vs_organic,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  cfs.channel,
  cfs.os,
  cfs.os_version,
  cfs.normalized_os,
  cfs.normalized_os_version,
  cfs.locale,
  cfs.app_version,
  cfs.windows_version,
  cfs.windows_build_number,
  COALESCE(au.is_dau, FALSE) AS is_dau,
  COUNT(cfs.client_id) AS new_profiles
FROM
  clients_first_seen cfs
LEFT JOIN
  active_users au
  ON cfs.client_id = au.client_id
  AND cfs.first_seen_date = au.submission_date
GROUP BY
  cfs.first_seen_date,
  cfs.is_desktop,
  cfs.attribution_medium,
  cfs.attribution_source,
  cfs.attribution_campaign,
  cfs.attribution_content,
  cfs.attribution_dlsource,
  cfs.attribution_ua,
  cfs.attributed,
  cfs.paid_vs_organic,
  cfs.city,
  cfs.country,
  cfs.distribution_id,
  cfs.channel,
  cfs.os,
  cfs.os_version,
  cfs.normalized_os,
  cfs.normalized_os_version,
  cfs.locale,
  cfs.app_version,
  cfs.windows_version,
  cfs.windows_build_number,
  au.is_dau
