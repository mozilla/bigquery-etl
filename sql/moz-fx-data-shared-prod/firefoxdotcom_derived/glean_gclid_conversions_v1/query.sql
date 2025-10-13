--Step 1: Get all combos of GCLID, GA Client ID, & Stub Session IDs Seen in Last 30 Days
WITH gclids_to_ga_ids AS (
  SELECT DISTINCT
    unnested_gclid AS gclid,
    ga_client_id,
    stub_session_id,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2`,
    UNNEST(gclid_array) AS unnested_gclid
  CROSS JOIN
    UNNEST(all_reported_stub_session_ids) AS stub_session_id
  WHERE
    session_date >= DATE_SUB(@submission_date, INTERVAL 30 DAY)
    -- Next line is needed for backfilling purposes
    AND session_date <= @submission_date
    AND gclid IS NOT NULL
),
--Step 2: Get all download tokens associated with above stub session / GA Client IDs
--        from the stub attribution logs
ga_ids_to_dl_token AS (
  SELECT DISTINCT
    a.ga_client_id,
    a.stub_session_id,
    a.dl_token,
    b.gclid
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1` a
  JOIN
    gclids_to_ga_ids b
    ON a.ga_client_id = b.ga_client_id
    AND a.stub_session_id = b.stub_session_id
  WHERE
    a.ga_client_id IS NOT NULL
    AND a.stub_session_id IS NOT NULL
),
--Step 3: Get unique download tokens (1 row per token)
dist_dl_tokens AS (
  SELECT DISTINCT
    dl_token
  FROM
    ga_ids_to_dl_token
),
--Step 4: Get the telemetry clent ID & first seen date for each download token
dl_token_to_telemetry_id AS (
  SELECT
    a.client_id AS telemetry_client_id,
    a.first_seen_date,
    a.attribution_dltoken AS dl_token,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen` a
  JOIN
    dist_dl_tokens b
    ON a.attribution_dltoken = b.dl_token
    AND a.submission_date <= @submission_date
),
--Step 5: Get the telemetry client ID, firefox first run date, # days running firefox, and most recent Firefox run date
old_events_from_bcd AS (
  SELECT
    a.telemetry_client_id,
    MIN(b.submission_date) AS firefox_first_run_date,
    COUNT(DISTINCT(b.submission_date)) AS nbr_days_running_firefox,
    MAX(b.submission_date) AS most_recent_date_running_firefox
  FROM
    dl_token_to_telemetry_id a
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1` b
    ON a.telemetry_client_id = b.client_id
    AND b.submission_date <= @submission_date
  GROUP BY
    a.telemetry_client_id
),
--Step 6: get firefox_first_ad_click_date, firefox_first_search_date,
old_events_from_mcd AS (
  SELECT
    a.telemetry_client_id,
    MIN(
      CASE
        WHEN IFNULL(b.ad_clicks_count_all, 0) > 0
          THEN b.submission_date
        ELSE NULL
      END
    ) AS firefox_first_ad_click_date,
    MIN(
      CASE
        WHEN IFNULL(b.search_count_all, 0) > 0
          THEN b.submission_date
        ELSE NULL
      END
    ) AS firefox_first_search_date
  FROM
    dl_token_to_telemetry_id a
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1` b
    ON a.telemetry_client_id = b.client_id
    AND b.submission_date <= @submission_date
  GROUP BY
    a.telemetry_client_id
),
--Step 7: Bring the relevant data into 1 table
old_events_combined AS (
  SELECT
    COALESCE(a.telemetry_client_id, b.telemetry_client_id) AS telemetry_client_id,
    a.firefox_first_run_date,
    a.nbr_days_running_firefox,
    a.most_recent_date_running_firefox,
    b.firefox_first_ad_click_date,
    b.firefox_first_search_date
  FROM
    old_events_from_bcd a
  FULL OUTER JOIN
    old_events_from_mcd b
    ON a.telemetry_client_id = b.telemetry_client_id
),
--Step 8: Summarize this into the old event types
--        If first run date = submission date, then it's firefox first run
--        If first ad click date = submission date, then it's first ad click date
--        If first search date = submission date, then it's firefox first search date
--        If # of days running firefox = 2 and most recent run date = submission date, then it's returned_second_day
old_events AS (
  SELECT
    telemetry_client_id,
    @submission_date AS activity_date,
    CASE
      WHEN @submission_date = firefox_first_run_date
        THEN TRUE
      ELSE FALSE
    END AS firefox_first_run,
    CASE
      WHEN @submission_date = firefox_first_ad_click_date
        THEN TRUE
      ELSE FALSE
    END AS firefox_first_ad_click,
    CASE
      WHEN @submission_date = firefox_first_search_date
        THEN TRUE
      ELSE FALSE
    END AS firefox_first_search,
    CASE
      WHEN most_recent_date_running_firefox = @submission_date
        AND nbr_days_running_firefox = 2
        THEN TRUE
      ELSE FALSE
    END AS returned_second_day
  FROM
    old_events_combined
),
--Step 7: Get all clients who were active on the activity date, and the type of activity they had
telemetry_id_to_activity_staging AS (
  SELECT
    client_id AS telemetry_client_id,
    report_date AS activity_date,
    CAST(NULL AS BOOLEAN) AS firefox_first_run,
    CAST(NULL AS BOOLEAN) AS firefox_first_ad_click,
    CAST(NULL AS BOOLEAN) AS firefox_first_search,
    CAST(NULL AS BOOLEAN) AS returned_second_day,
    event_1 AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    event_2 AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    event_3 AS first_wk_3_actv_days_and_24_active_minutes,
    is_dau_at_least_4_of_first_7_days,
    is_dau_at_least_3_of_first_7_days,
    is_dau_at_least_2_of_first_7_days,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v2`
  WHERE
    (
      event_1 IS TRUE
      OR event_2 IS TRUE
      OR event_3 IS TRUE
      OR is_dau_at_least_4_of_first_7_days IS TRUE
      OR is_dau_at_least_3_of_first_7_days IS TRUE
      OR is_dau_at_least_2_of_first_7_days IS TRUE
    )
    AND report_date = @submission_date
    AND first_seen_date < @submission_date --needed since this is a required partition filter
  UNION ALL
  SELECT
    telemetry_client_id,
    activity_date,
    firefox_first_run,
    firefox_first_ad_click,
    firefox_first_search,
    returned_second_day,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
    CAST(NULL AS BOOLEAN) AS is_dau_at_least_4_of_first_7_days,
    CAST(NULL AS BOOLEAN) AS is_dau_at_least_3_of_first_7_days,
    CAST(NULL AS BOOLEAN) AS is_dau_at_least_2_of_first_7_days,
  FROM
    old_events
  WHERE
    firefox_first_run IS TRUE
    OR firefox_first_ad_click IS TRUE
    OR firefox_first_ad_click IS TRUE
    OR returned_second_day IS TRUE
),
--Step 8 - Aggregate to 1 row per client ID/activity date
telemetry_id_to_activity AS (
  SELECT
    telemetry_client_id,
    activity_date,
    MAX(
      COALESCE(first_wk_5_actv_days_and_1_or_more_search_w_ads, FALSE)
    ) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    MAX(
      COALESCE(first_wk_3_actv_days_and_1_or_more_search_w_ads, FALSE)
    ) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    MAX(
      COALESCE(first_wk_3_actv_days_and_24_active_minutes, FALSE)
    ) AS first_wk_3_actv_days_and_24_active_minutes,
    MAX(COALESCE(is_dau_at_least_4_of_first_7_days, FALSE)) AS is_dau_at_least_4_of_first_7_days,
    MAX(COALESCE(is_dau_at_least_3_of_first_7_days, FALSE)) AS is_dau_at_least_3_of_first_7_days,
    MAX(COALESCE(is_dau_at_least_2_of_first_7_days, FALSE)) AS is_dau_at_least_2_of_first_7_days,
    MAX(COALESCE(firefox_first_run, FALSE)) AS firefox_first_run,
    MAX(COALESCE(firefox_first_ad_click, FALSE)) AS firefox_first_ad_click,
    MAX(COALESCE(firefox_first_search, FALSE)) AS firefox_first_search,
    MAX(COALESCE(returned_second_day, FALSE)) AS returned_second_day
  FROM
    telemetry_id_to_activity_staging
  GROUP BY
    telemetry_client_id,
    activity_date
)
--Step 9: Get Click IDs and associated events on this activity date
SELECT
  activity_date,
  gclid,
  MAX(COALESCE(firefox_first_run, FALSE)) AS did_firefox_first_run,
  MAX(COALESCE(firefox_first_search, FALSE)) AS did_search,
  MAX(COALESCE(firefox_first_ad_click, FALSE)) AS did_click_ad,
  MAX(COALESCE(returned_second_day, FALSE)) AS did_returned_second_day,
  MAX(
    COALESCE(first_wk_5_actv_days_and_1_or_more_search_w_ads, FALSE)
  ) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
  MAX(
    COALESCE(first_wk_3_actv_days_and_1_or_more_search_w_ads, FALSE)
  ) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
  MAX(
    COALESCE(first_wk_3_actv_days_and_24_active_minutes, FALSE)
  ) AS first_wk_3_actv_days_and_24_active_minutes,
  MAX(COALESCE(is_dau_at_least_4_of_first_7_days, FALSE)) AS is_dau_at_least_4_of_first_7_days,
  MAX(COALESCE(is_dau_at_least_3_of_first_7_days, FALSE)) AS is_dau_at_least_3_of_first_7_days,
  MAX(COALESCE(is_dau_at_least_2_of_first_7_days, FALSE)) AS is_dau_at_least_2_of_first_7_days,
FROM
  ga_ids_to_dl_token
INNER JOIN
  dl_token_to_telemetry_id
  USING (dl_token)
INNER JOIN
  telemetry_id_to_activity
  USING (telemetry_client_id)
GROUP BY
  activity_date,
  gclid
