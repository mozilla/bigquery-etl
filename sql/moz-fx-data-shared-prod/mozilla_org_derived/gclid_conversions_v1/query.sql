WITH gclids_to_ga_ids AS (
  SELECT
    gclid,
    ga_client_id,
    stub_session_id,
  FROM
    mozilla_org_derived.ga_sessions_v1
  CROSS JOIN
    UNNEST(all_reported_stub_session_ids) AS stub_session_id
  WHERE
    session_date >= DATE_SUB(@activity_date, INTERVAL @conversion_window DAY)
    AND gclid IS NOT NULL
),
ga_ids_to_dl_token AS (
  SELECT
    ga_client_id,
    stub_session_id,
    dl_token,
  FROM
    stub_attribution_service_derived.dl_ga_triplets_v1
  WHERE
    ga_client_id IS NOT NULL
    AND stub_session_id IS NOT NULL
),
dl_token_to_telemetry_id AS (
  SELECT
    client_id AS telemetry_client_id,
    first_seen_date,
    attribution_dltoken AS dl_token,
  FROM
    telemetry_derived.clients_first_seen_v2
),
telemetry_id_to_activity AS (
  SELECT
    client_id AS telemetry_client_id,
    @activity_date AS activity_date,
    search_count_all > 0 AS did_search,
    ad_clicks_count_all > 0 AS did_click_ad,
    TRUE AS was_active,
  FROM
    telemetry_derved.clients_daily_v6
  WHERE
    submission_date = @activity_date
)
SELECT
  activity_date,
  gclid,
  COALESCE(LOGICAL_OR(did_search), FALSE) AS did_search,
  COALESCE(LOGICAL_OR(did_click_ad), FALSE) AS did_click_ad,
  COALESCE(
    LOGICAL_OR(was_active AND activity_date > first_seen_date),
    FALSE
  ) AS did_returned_second_day
FROM
  gclids_to_ga_ids
INNER JOIN
  ga_ids_to_dl_token
USING
  (ga_client_id, stub_session_id)
INNER JOIN
  dl_token_to_telemetry_id
USING
  (dl_token)
INNER JOIN
  telemetry_id_to_activity
USING
  (telemetry_client_id)
GROUP BY
  gclid
HAVING
  did_search
  OR did_click_ad
  OR did_returned_second_day
