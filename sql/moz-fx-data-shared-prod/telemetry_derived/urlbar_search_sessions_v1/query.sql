WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    timestamp AS event_timestamp,
    normalized_channel,
    normalized_country_code,
    name AS event_name,
    CASE
      WHEN name = 'engagement'
        AND (
          (
            mozfun.map.get_key(extra, "selected_result") IN (
              "click",
              "drop_go",
              "enter",
              "go_button",
              "paste_go"
            )
          )
        )
        THEN 'engaged'
      WHEN name = 'engagement'
        AND (
          (
            mozfun.map.get_key(extra, "selected_result") NOT IN (
              "click",
              "drop_go",
              "enter",
              "go_button",
              "paste_go"
            )
          )
        )
        OR name = 'abandonment'
        THEN 'abandoned'
      WHEN name = 'engagement'
        AND (
          (
            mozfun.map.get_key(extra, "selected_result") NOT IN (
              "click",
              "drop_go",
              "enter",
              "go_button",
              "paste_go"
            )
          )
        )
        THEN "annoyance"
      ELSE NULL
    END AS search_session_type,
    SPLIT(mozfun.map.get_key(extra, "results"), ',')[OFFSET(0)] AS result_type,
    SPLIT(mozfun.map.get_key(extra, "results"), ',') AS results,
    mozfun.map.get_key(extra, "selected_result") AS selected_results,
    mozfun.map.get_key(extra, "n_chars") AS number_of_chars_typed,
    metrics,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'urlbar'
    AND name IN ('engagement', 'abadonment')
),
events_summary AS (
  SELECT
    submission_date,
    client_id,
    event_timestamp,
    normalized_channel,
    normalized_country_code,
    event_name,
    search_session_type,
    CASE
      WHEN search_session_type = 'engaged'
        THEN selected_results
      ELSE NULL
    END AS engaged_result_type,
    result_type,
    number_of_chars_typed,
    CASE
      WHEN search_session_type = 'annoyance'
        THEN selected_results
      ELSE NULL
    END AS annoyance_signal_type,
    COUNTIF(
      res IN ('search_engine', 'search_history', 'trending_search', 'trending_search_rich')
    ) AS num_search_engine_impressions,
    COUNTIF(res IN ('history', 'search_history')) AS num_history_impressions,
    COUNTIF(
      res IN (
        'merino_wikipedia',
        'merino_adm_nonsponsored',
        'merino_top_picks',
        'rs_ad_nonsponsored',
        'search_suggest',
        'search_suggest_rich',
        'suggest_non_sponsor'
      )
    ) AS num_wikipedia_impressions,
    COUNTIF(res IN ('weather')) AS num_weather_impressions,
    COUNTIF(
      res IN (
        'merino_adm_sponsored',
        'merino_top_picks',
        'rs_adm_sponsored',
        'search_suggest',
        'search_suggest_rich',
        'suggest_sponsor'
      )
    ) AS num_adm_sponsored_impressions,
    legacy_telemetry_client_id,
    client_id AS glean_metrics_client_id,
  FROM
    events_unnested,
    UNNEST(results) AS res
  GROUP BY
    submission_date,
    client_id,
    legacy_telemetry_client_id,
    event_timestamp,
    normalized_channel,
    normalized_country_code,
    event_name,
    result_type,
    selected_results,
    search_session_type,
    number_of_chars_typed
),
legacy_profile_info AS (
  SELECT
    client_id AS legacy_telemetry_client_id,
    user_pref_browser_urlbar_quicksuggest_data_collection_enabled AS sharing_enabled,
    user_pref_browser_urlbar_suggest_quicksuggest_sponsored AS sponsored_suggestion_enabled,
    IF(user_pref_browser_urlbar_suggest_quicksuggest = 'true', TRUE, FALSE)
    OR IF(
      user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored = 'false',
      FALSE,
      TRUE
    ) AS suggest_enabled,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id,
    sharing_enabled,
    sponsored_suggestion_enabled,
    suggest_enabled
),
glean_metrics_info AS (
  SELECT
    MIN(submission_timestamp) AS submission_timestamp,
    client_info.client_id AS glean_metrics_client_id,
    udf.normalize_search_engine(
      metrics.string.search_engine_default_engine_id
    ) AS normalized_search_engine,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    glean_metrics_client_id,
    normalized_search_engine
  ORDER BY
    submission_timestamp
)
SELECT
  submission_date,
  client_id AS glean_client_id,
  event_timestamp,
  normalized_channel,
  normalized_country_code,
  event_name,
  search_session_type,
  engaged_result_type,
  annoyance_signal_type,
  result_type AS first_result_type,
  number_of_chars_typed,
  num_search_engine_impressions,
  num_history_impressions,
  num_wikipedia_impressions,
  num_weather_impressions,
  num_adm_sponsored_impressions,
  sharing_enabled,
  sponsored_suggestion_enabled,
  suggest_enabled,
  normalized_search_engine,
  legacy_telemetry_client_id,
FROM
  events_summary
LEFT JOIN
  legacy_profile_info
USING
  (legacy_telemetry_client_id)
LEFT JOIN
  glean_metrics_info
USING
  (glean_metrics_client_id)
