WITH base AS (
  SELECT
    client_id,
    sample_id,
    submission_date,
    first_seen_date,
    days_since_first_seen,
    days_since_seen,
    days_seen_bytes,
    durations
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline_clients_yearly`
  WHERE
    submission_date = @submission_date
    AND NOT (
      BIT_COUNT(days_seen_bytes) != 1
      AND DATE_DIFF(submission_date, first_seen_date, DAY) = 0
    )
),
ad_clicks AS (
  SELECT
    b.client_id,
    b.sample_id,
    b.submission_date,
    b.first_seen_date,
    b.days_since_first_seen,
    b.days_since_seen,
    b.days_seen_bytes,
    b.durations,
    (
      SELECT
        LEAST(value, 10000)
      FROM
        UNNEST(ad_click_history)
      WHERE
        key = b.submission_date
    ) AS ad_clicks,
    (
      SELECT
        SUM(LEAST(value, 10000))
      FROM
        UNNEST(ad_click_history)
      WHERE
        key <= b.submission_date
    ) AS total_historic_ad_clicks
  FROM
    base b
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_ios.client_adclicks_history` ad_clck_hist
    USING (client_id, sample_id)
)
SELECT
  ac.client_id,
  ac.sample_id,
  ac.submission_date,
  ac.first_seen_date,
  ac.days_since_first_seen,
  ac.days_since_seen,
  ac.days_seen_bytes,
  ac.durations,
  ac.ad_clicks,
  ac.total_historic_ad_clicks,
  clients.adjust_network,
  clients.country AS first_reported_country,
  CAST(NULL AS STRING) AS first_reported_isp,
FROM
  ad_clicks ac
JOIN
  `moz-fx-data-shared-prod.firefox_ios.new_profile_clients` AS clients
  USING (client_id)
WHERE
    -- BrowserStack clients are bots, we don't want to accidentally report on them
  app_name <> "Firefox iOS BrowserStack"
    -- Remove clients who are new on this day, but have more/less than 1 day of activity
  AND NOT (days_since_first_seen = 0 AND BIT_COUNT(days_seen_bytes) != 1)
