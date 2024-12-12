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
    session_date >= DATE_SUB(@submission_date, INTERVAL @conversion_window DAY)
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
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3`
),
--Step 4: Get all clients who were active on the activity date, and the type of activity they had
telemetry_id_to_activity_staging AS (
  SELECT
    client_id AS telemetry_client_id,
    submission_date AS activity_date,
    IFNULL(search_count_all, 0) > 0 AS did_search,
    IFNULL(ad_clicks_count_all, 0) > 0 AS did_click_ad,
    CAST(NULL AS BOOLEAN) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    CAST(NULL AS BOOLEAN) AS first_wk_3_actv_days_and_24_active_minutes,
    TRUE AS was_active,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    client_id AS telemetry_client_id,
    report_date AS activity_date,
    CAST(NULL AS BOOLEAN) AS did_search,
    CAST(NULL AS BOOLEAN) AS did_click_ad,
    event_1 AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    event_2 AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    event_3 AS first_wk_3_actv_days_and_24_active_minutes,
    CAST(NULL AS BOOLEAN) AS was_active
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v1`
  WHERE
    (event_1 IS TRUE OR event_2 IS TRUE OR event_3 IS TRUE)
    AND report_date = @submission_date
    AND first_seen_date < @submission_date --needed since this is a required partition filter
),
telemetry_id_to_activity AS (
  SELECT
    telemetry_client_id,
    activity_date,
    MAX(COALESCE(did_search, FALSE)) AS did_search,
    MAX(COALESCE(did_click_ad, FALSE)) AS did_click_ad,
    MAX(
      COALESCE(first_wk_5_actv_days_and_1_or_more_search_w_ads, FALSE)
    ) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
    MAX(
      COALESCE(first_wk_3_actv_days_and_1_or_more_search_w_ads, FALSE)
    ) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
    MAX(
      COALESCE(first_wk_3_actv_days_and_24_active_minutes, FALSE)
    ) AS first_wk_3_actv_days_and_24_active_minutes,
    MAX(COALESCE(was_active, FALSE)) AS was_active
  FROM
    telemetry_id_to_activity_staging
  GROUP BY
    telemetry_client_id,
    activity_date
)
--Step 5: Get Click IDs and associated events on this activity date (using previous defined events)
SELECT
  activity_date,
  gclid,
  COALESCE(
    LOGICAL_OR(was_active AND activity_date = first_seen_date),
    FALSE
  ) AS did_firefox_first_run,
  COALESCE(LOGICAL_OR(did_search), FALSE) AS did_search,
  COALESCE(LOGICAL_OR(did_click_ad), FALSE) AS did_click_ad,
  COALESCE(
    LOGICAL_OR(was_active AND activity_date > first_seen_date),
    FALSE
  ) AS did_returned_second_day,
  COALESCE(
    LOGICAL_OR(first_wk_5_actv_days_and_1_or_more_search_w_ads),
    FALSE
  ) AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
  COALESCE(
    LOGICAL_OR(first_wk_3_actv_days_and_1_or_more_search_w_ads),
    FALSE
  ) AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
  COALESCE(
    LOGICAL_OR(first_wk_3_actv_days_and_24_active_minutes),
    FALSE
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
