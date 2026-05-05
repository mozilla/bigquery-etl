WITH last_28_days AS (
  SELECT DISTINCT
    submission_date AS submission_dt
  FROM
    `moz-fx-data-shared-prod.telemetry.segmented_dau`
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
),
raw_dau_last_28_days AS (
  --note: even though table is already currently grouped by these,
  --here we re-doing the group by in case columns are added in
  --the future to this view, so as to not introduce dupes
  SELECT
    submission_date,
    country,
    app_name,
    adjust_network,
    attribution_medium,
    attribution_source,
    first_seen_year,
    channel,
    install_source,
    is_default_browser,
    os_grouped,
    segment,
    SUM(dau) AS dau
  FROM
    `moz-fx-data-shared-prod.telemetry.segmented_dau` sd
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND @submission_date
  GROUP BY
    submission_date,
    country,
    app_name,
    adjust_network,
    attribution_medium,
    attribution_source,
    first_seen_year,
    channel,
    install_source,
    is_default_browser,
    os_grouped,
    segment
),
all_combos_with_any_date_having_dau_in_last_28_days AS (
  SELECT DISTINCT
    country,
    app_name,
    adjust_network,
    attribution_medium,
    attribution_source,
    first_seen_year,
    channel,
    install_source,
    is_default_browser,
    os_grouped,
    segment,
  FROM
    raw_dau_last_28_days
),
all_day_group_combos AS (
  SELECT
    a.country,
    a.app_name,
    a.adjust_network,
    a.attribution_medium,
    a.attribution_source,
    a.first_seen_year,
    a.channel,
    a.install_source,
    a.is_default_browser,
    a.os_grouped,
    a.segment,
    b.submission_dt
  FROM
    all_combos_with_any_date_having_dau_in_last_28_days a
  CROSS JOIN
    last_28_days b
),
daily_dau_last_28_days AS (
  SELECT
    a.country,
    a.app_name,
    a.adjust_network,
    a.attribution_medium,
    a.attribution_source,
    a.first_seen_year,
    a.channel,
    a.install_source,
    a.is_default_browser,
    a.os_grouped,
    a.segment,
    a.submission_dt AS submission_date,
    COALESCE(b.dau, 0) AS dau,
    AVG(COALESCE(b.dau, 0)) OVER (
      PARTITION BY
        a.country,
        a.app_name,
        a.adjust_network,
        a.attribution_medium,
        a.attribution_source,
        a.first_seen_year,
        a.channel,
        a.install_source,
        a.is_default_browser,
        a.os_grouped,
        a.segment
      ORDER BY
        a.submission_dt
      ROWS BETWEEN
        27 PRECEDING
        AND CURRENT ROW
    ) AS ma_28_dau
  FROM
    all_day_group_combos a
  LEFT JOIN
    raw_dau_last_28_days b
    ON COALESCE(a.country, 'NULL') = COALESCE(b.country, 'NULL')
    AND COALESCE(a.app_name, 'NULL') = COALESCE(b.app_name, 'NULL')
    AND COALESCE(a.adjust_network, 'NULL') = COALESCE(b.adjust_network, 'NULL')
    AND COALESCE(a.attribution_medium, 'NULL') = COALESCE(b.attribution_medium, 'NULL')
    AND COALESCE(a.attribution_source, 'NULL') = COALESCE(b.attribution_source, 'NULL')
    AND COALESCE(a.first_seen_year, -123) = COALESCE(b.first_seen_year, -123)
    AND COALESCE(a.channel, 'NULL') = COALESCE(b.channel, 'NULL')
    AND COALESCE(a.install_source, 'NULL') = COALESCE(b.install_source, 'NULL')
    AND COALESCE(CAST(a.is_default_browser AS string), 'NULL') = COALESCE(
      CAST(b.is_default_browser AS string),
      'NULL'
    )
    AND COALESCE(a.os_grouped, 'NULL') = COALESCE(b.os_grouped, 'NULL')
    AND COALESCE(a.segment, 'NULL') = COALESCE(b.segment, 'NULL')
    AND a.submission_dt = b.submission_date
  QUALIFY
    RANK() OVER (ORDER BY a.submission_dt DESC) = 1
)
SELECT
  @submission_date AS submission_date,
  country,
  app_name,
  adjust_network,
  attribution_medium,
  attribution_source,
  first_seen_year,
  channel,
  install_source,
  is_default_browser,
  os_grouped,
  segment,
  dau,
  ma_28_dau
FROM
  daily_dau_last_28_days
