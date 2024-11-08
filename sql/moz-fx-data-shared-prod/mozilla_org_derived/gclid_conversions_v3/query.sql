--Step 1: Get all combinations of google click IDs, google analytics client IDs, and stub session IDs in last 30 days
WITH gclids_to_ga_ids AS (
  SELECT DISTINCT
    unnested_gclid AS gclid,
    ga_client_id,
    stub_session_id,
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2`,
    UNNEST(gclid_array) AS unnested_gclid
  CROSS JOIN
    UNNEST(all_reported_stub_session_ids) AS stub_session_id
  WHERE
    session_date >= DATE_SUB(@submission_date, INTERVAL 30 DAY)
    -- Next line is needed for backfilling purposes
    AND session_date <= @submission_date
    AND gclid IS NOT NULL
),
--Step 2: Get all the download tokens associated with a known GA client ID & stub session ID
ga_ids_to_dl_token AS (
  SELECT
    ga_client_id,
    stub_session_id,
    dl_token,
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
  WHERE
    ga_client_id IS NOT NULL
    AND stub_session_id IS NOT NULL
),
--Step 3: Get the telemetry clent ID & first seen date for each download token
dl_token_to_telemetry_id AS (
  SELECT
    client_id AS telemetry_client_id,
    first_seen_date,
    attribution_dltoken AS dl_token,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
),
--Step 4: Get the new conversion event types from conversion_event_categorization_v1
new_conversion_events AS (
  SELECT
    client_id AS telemetry_client_id,
    report_date AS activity_date,
    event_1 AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    event_2 AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    event_3 AS first_wk_3_actv_days_and_24_active_minutes
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v1`
  WHERE
    (event_1 IS TRUE OR event_2 IS TRUE OR event_3 IS TRUE)
    AND report_date = @submission_date
    AND first_seen_date < @submission_date
),
--Step 5: Get all clients who had their first run on the report date
firefox_first_run AS (
  SELECT
    client_id AS telemetry_client_id,
    TRUE AS firefox_first_run,
    MIN(submission_date) AS activity_date
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  GROUP BY
    1,
    2
  HAVING
    MIN(submission_date) = @submission_date
),
--Step 6: Get all clients who had their first ever ad click on the report date
firefox_first_ad_click AS (
  SELECT
    client_id AS telemetry_client_id,
    TRUE AS firefox_first_ad_click,
    MIN(submission_date) AS activity_date
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    IFNULL(ad_clicks_count_all, 0) > 0
  GROUP BY
    1,
    2
  HAVING
    MIN(submission_date) = @submission_date
),
--Step 7: Get all clients who had their first ever search on the report date
firefox_first_search AS (
  SELECT
    client_id AS telemetry_client_id,
    TRUE AS firefox_first_search,
    MIN(submission_date) AS activity_date
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    IFNULL(search_count_all, 0) > 0
  GROUP BY
    1,
    2
  HAVING
    MIN(submission_date) = @submission_date
),
--Step 8: Get all clients who returned a second day for the first time on the report date
returned_second_day AS (
  SELECT
    client_id AS telemetry_client_id,
    TRUE AS returned_second_day,
    @submission_date AS activity_date,
    COUNT(DISTINCT(submission_date)) AS nbr_active_days
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  GROUP BY
    1,
    2,
    3
  HAVING
    COUNT(DISTINCT(submission_date)) = 2
    AND MAX(submission_date) = @submission_date
),
--Step 9: Get all clients who were active on the activity date, and the type of activity they had
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
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v1`
  WHERE
    (event_1 IS TRUE OR event_2 IS TRUE OR event_3 IS TRUE)
    AND report_date = @submission_date
    AND first_seen_date < @submission_date --needed since this is a required partition filter
  UNION ALL
  SELECT
    telemetry_client_id,
    activity_date,
    firefox_first_run,
    CAST(NULL AS BOOLEAN) AS firefox_first_ad_click,
    CAST(NULL AS BOOLEAN) AS firefox_first_search,
    CAST(NULL AS BOOLEAN) AS returned_second_day,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
  FROM
    firefox_first_run
  UNION ALL
  SELECT
    telemetry_client_id,
    activity_date,
    CAST(NULL AS BOOLEAN) AS firefox_first_run,
    firefox_first_ad_click,
    CAST(NULL AS BOOLEAN) AS firefox_first_search,
    CAST(NULL AS BOOLEAN) AS returned_second_day,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
  FROM
    firefox_first_ad_click
  UNION ALL
  SELECT
    telemetry_client_id,
    activity_date,
    CAST(NULL AS BOOLEAN) AS firefox_first_run,
    CAST(NULL AS BOOLEAN) AS firefox_first_ad_click,
    firefox_first_search,
    CAST(NULL AS BOOLEAN) AS returned_second_day,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
  FROM
    firefox_first_search
  UNION ALL
  SELECT
    telemetry_client_id,
    activity_date,
    CAST(NULL AS BOOLEAN) AS firefox_first_run,
    CAST(NULL AS BOOLEAN) AS firefox_first_ad_click,
    CAST(NULL AS BOOLEAN) AS firefox_first_search,
    returned_second_day,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
  FROM
    returned_second_day
),
--Step 10 - Aggregate to 1 row per client ID/activity date
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
--Step 11: Get Click IDs and associated events on this activity date
SELECT
  activity_date,
  gclid,
  MAX(COALESCE(firefox_first_run, FALSE)) AS firefox_first_run,
  MAX(COALESCE(firefox_first_search, FALSE)) AS firefox_first_search,
  MAX(COALESCE(firefox_first_ad_click, FALSE)) AS firefox_first_ad_click,
  MAX(COALESCE(returned_second_day, FALSE)) AS firefox_active_a_second_day,
  MAX(
    COALESCE(first_wk_5_actv_days_and_1_or_more_search_w_ads, FALSE)
  ) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
  MAX(
    COALESCE(first_wk_3_actv_days_and_1_or_more_search_w_ads, FALSE)
  ) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
  MAX(
    COALESCE(first_wk_3_actv_days_and_24_active_minutes, FALSE)
  ) AS first_wk_3_actv_days_and_24_active_minutes
FROM
  gclids_to_ga_ids
INNER JOIN
  ga_ids_to_dl_token
  USING (ga_client_id, stub_session_id)
INNER JOIN
  dl_token_to_telemetry_id
  USING (dl_token)
INNER JOIN
  telemetry_id_to_activity
  USING (telemetry_client_id)
GROUP BY
  activity_date,
  gclid
