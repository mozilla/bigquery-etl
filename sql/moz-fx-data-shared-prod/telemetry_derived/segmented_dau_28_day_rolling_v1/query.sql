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
    b.submission_dt AS submission_date,
    COALESCE(c.dau, 0) AS dau,
    AVG(COALESCE(c.dau, 0)) OVER (
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
        b.submission_dt
      ROWS BETWEEN
        27 PRECEDING
        AND CURRENT ROW
    ) AS ma_28_dau
  FROM
    all_combos_with_any_date_having_dau_in_last_28_days a
  CROSS JOIN
    last_28_days b
  LEFT JOIN
    raw_dau_last_28_days c
    ON a.country = c.country
    AND a.app_name = c.app_name
    AND a.adjust_network = c.adjust_network
    AND a.attribution_medium = c.attribution_medium
    AND a.attribution_source = c.attribution_source
    AND a.first_seen_year = c.first_seen_year
    AND a.channel = c.channel
    AND a.install_source = c.install_source
    AND a.is_default_browser = c.is_default_browser
    AND a.os_grouped = c.os_grouped
    AND a.segment = c.segment
    AND b.submission_dt = c.submission_date
  QUALIFY
    RANK() OVER (ORDER BY submission_date DESC) = 1
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
