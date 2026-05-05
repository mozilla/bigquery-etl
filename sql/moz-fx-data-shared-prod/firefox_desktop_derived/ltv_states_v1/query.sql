WITH base AS (
  SELECT
    cls.client_id,
    cls.sample_id,
    cls.submission_date,
    DATE_DIFF(cls.submission_date, cfs.first_seen_date, DAY) AS days_since_first_seen,
    cfs.first_seen_date AS first_seen_date,
    cls.first_reported_country,
    cfs.attribution,
    cls.days_seen_bytes,
    `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(cls.days_seen_bytes) AS days_since_active,
    IF(
      (
        (cls.total_uri_count_sum > 0)
        AND (`moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(cls.days_seen_bytes) = 0)
        AND (cls.active_hours_sum > 0)
      ),
      1,
      0
    ) AS active,
  FROM
    `moz-fx-data-shared-prod.search.search_clients_last_seen_v2` cls
  JOIN
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v1` cfs
    USING (client_id)
  WHERE
    cls.submission_date = @submission_date
    AND cls.submission_date >= cfs.first_seen_date  -- remove cases where the last time a client was seen was before their supposed first date
    -- days since last seen should be less than or equal to days since first seen
    AND `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(cls.days_seen_bytes) <= DATE_DIFF(
      cls.submission_date,
      cfs.first_seen_date,
      DAY
    )
    AND cls.days_seen_bytes IS NOT NULL
)
SELECT
  b.client_id,
  b.submission_date,
  b.sample_id,
  b.first_seen_date,
  b.days_since_first_seen,
  b.days_since_active,
  b.first_reported_country,
  b.attribution,
  b.days_seen_bytes,
  b.active,
  (
    SELECT
      LEAST(value, 10000)
    FROM
      UNNEST(ad_click_history)
    WHERE
      key = @submission_date
  ) AS ad_clicks,
  (
    SELECT
      SUM(LEAST(value, 10000))
    FROM
      UNNEST(ad_click_history)
    WHERE
      key <= @submission_date
  ) AS total_historic_ad_clicks
FROM
  base b
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` h
  USING (client_id)
