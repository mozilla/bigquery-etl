WITH events_unnested AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_info.client_id,
    timestamp AS event_timestamp,
    normalized_channel,
    normalized_country_code,
    name AS event_name,
    CASE
      WHEN name = 'engagement'
        AND (
          (
            mozfun.map.get_key(extra, "engagement_type") IN (
              "click",
              "drop_go",
              "enter",
              "go_button",
              "paste_go"
            )
          )
        )
        THEN 'engaged'
      WHEN name = 'abandonment'
        THEN 'abandoned'
      WHEN name = 'engagement'
        AND (
          (
            mozfun.map.get_key(extra, "engagement_type") NOT IN (
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
    mozfun.map.get_key(extra, "engagement_type") AS engagement_type,
    mozfun.map.get_key(extra, "n_chars") AS number_of_chars_typed,
    mozfun.map.get_key(extra, "n_results") AS num_total_results,
    metrics,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    ping_info.experiments
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events)
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'urlbar'
    AND name IN ('engagement', 'abadonment')
),
events_summary AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    event_timestamp,
    normalized_channel,
    normalized_country_code,
    event_name,
    search_session_type,
    CASE
      WHEN search_session_type = 'engaged'
        THEN engagement_type
      ELSE NULL
    END AS engaged_result_type,
    result_type,
    number_of_chars_typed,
    num_total_results,
    CASE
      WHEN search_session_type = 'annoyance'
        THEN engagement_type
      ELSE NULL
    END AS annoyance_signal_type,
    COUNTIF(
      res IN ('search_suggest', 'search_history', 'search_suggest_rich')
    ) AS num_default_partner_search_suggestions,
    COUNTIF(res IN ('searchengine')) AS num_search_engine_suggestions_impressions,
    COUNTIF(
      res IN ('trending_search', 'trending_search_rich')
    ) AS num_trending_suggestions_impressions,
    COUNTIF(res IN ('history')) AS num_history_impressions,
    COUNTIF(res IN ('bookmark', 'keyword')) AS num_bookmarks_impressions,
    COUNTIF(res IN ('tabs')) AS num_open_tabs_impressions,
    COUNTIF(
      res IN ('merino_adm_sponsored', 'rs_adm_sponsored', 'suggest_sponsor')
    ) AS admarketplace_sponsored_impressions,
    COUNTIF(res IN ('merino_top_picks')) AS num_navigations_impressions,
    COUNTIF(res IN ('addon', 'rs_amo')) AS num_add_on_impressions,
    COUNTIF(
      res IN ('rs_adm_nonsponsored', 'merino_adm_nonsponsored', 'suggest_non_sponsor')
    ) AS num_wikipedia_enhanced_impressions,
    COUNTIF(res IN ('dynamic_wikipedia', 'merino_wikipedia')) AS num_wikipedia_dynamic_impressions,
    COUNTIF(res IN ('weather')) AS num_weather_impressions,
    COUNTIF(
      res IN (
        'action',
        'intervention_clear',
        'intervention_refresh',
        'intervention_unknown',
        'intervention_update'
      )
    ) AS num_quick_actions_impressions,
    COUNTIF(res IN ('rs_pocket')) AS num_pocket_collection_impressions,
    legacy_telemetry_client_id,
    client_id AS glean_metrics_client_id,
    ARRAY_CONCAT_AGG(experiments) AS experiments
  FROM
    events_unnested,
    UNNEST(results) AS res
  GROUP BY
    submission_date,
    sample_id,
    client_id,
    legacy_telemetry_client_id,
    event_timestamp,
    normalized_channel,
    normalized_country_code,
    event_name,
    result_type,
    selected_results,
    engagement_type,
    search_session_type,
    number_of_chars_typed,
    num_total_results
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
  num_total_results,
  num_default_partner_search_suggestions,
  num_search_engine_suggestions_impressions,
  num_trending_suggestions_impressions,
  num_history_impressions,
  num_open_tabs_impressions,
  num_bookmarks_impressions,
  admarketplace_sponsored_impressions,
  num_navigations_impressions,
  num_add_on_impressions,
  num_wikipedia_enhanced_impressions,
  num_wikipedia_dynamic_impressions,
  num_weather_impressions,
  num_quick_actions_impressions,
  num_pocket_collection_impressions,
  sharing_enabled,
  sponsored_suggestion_enabled,
  suggest_enabled,
  normalized_search_engine,
  legacy_telemetry_client_id,
  experiments
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
