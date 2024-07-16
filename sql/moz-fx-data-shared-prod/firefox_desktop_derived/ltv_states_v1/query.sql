WITH base AS (
  SELECT
    cls.client_id,
    cls.sample_id,
    cls.submission_date,
    DATE_DIFF(cls.submission_date, cfs.first_seen_date, DAY) AS days_since_first_seen, 
    cfs.first_seen_date AS first_seen_date,
    first_reported_country,
    cfs.attribution,
    `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) AS days_since_active,
    IF(((total_uri_count_sum > 0) AND (`moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) = 0) AND (cls.active_hours_sum > 0)), 1, 0) AS active,
    IF(`moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) = 0, ad_click, 0) AS ad_clicks,
    FROM `moz-fx-data-shared-prod.search.search_clients_last_seen_v2` cls
    JOIN `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v1` cfs
    USING(client_id, sample_id)
    WHERE sample_id >= min_sample_id
    AND sample_id <= max_sample_id
    AND cls.submission_date >= first_cohort_date
    AND cls.submission_date <= last_observation_date
    AND cfs.first_seen_date >= first_cohort_date -- clients newer than first cohort we care about
    AND cfs.first_seen_date <= last_cohort_date -- clients not any newer than that last cohort
    AND cls.submission_date >= cfs.first_seen_date  -- to account for some bad clients        
    -- remove cases where the last time a client was seen was before their supposed first date
    AND `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) <= DATE_DIFF(cls.submission_date, cfs.first_seen_date, DAY)
    AND days_seen_bytes IS NOT NULL
)

SELECT
b.client_id,
b.sample_id,
b.submission_date,
b.first_seen_date,
b.days_since_first_seen,
b.days_since_active,
b.first_reported_country, 
b.attribution,
b.active,
b.ad_clicks,
h.total_historic_ad_clicks
FROM base b
LEFT JOIN `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` h
USING (client_id)
