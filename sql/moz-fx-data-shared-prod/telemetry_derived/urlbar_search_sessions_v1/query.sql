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
    `mozfun.norm.result_type_to_product_name`(
      SPLIT(mozfun.map.get_key(extra, "results"), ',')[OFFSET(0)]
    ) AS product_result_type,
    SPLIT(mozfun.map.get_key(extra, "results"), ',') AS results,
    ARRAY(
      SELECT
        `mozfun.norm.result_type_to_product_name`(x)
      FROM
        UNNEST(SPLIT(mozfun.map.get_key(extra, "results"), ',')) AS x
    ) AS product_results,
    mozfun.map.get_key(extra, "selected_result") AS selected_result,
    `mozfun.norm.result_type_to_product_name`(
      mozfun.map.get_key(extra, "selected_result")
    ) AS product_selected_result,
    mozfun.map.get_key(extra, "engagement_type") AS engagement_type,
    CAST(mozfun.map.get_key(extra, "n_chars") AS int) AS num_chars_typed,
    CAST(mozfun.map.get_key(extra, "n_results") AS int) AS num_total_results,
    metrics,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    ping_info.experiments
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'urlbar'
    AND name IN ('engagement', 'abandonment')
),
--remove events where the urlbar dropdown menu remains open (i.e., the urlbar session did not end)
intermediate_states_removed AS (
  SELECT *
  FROM events_unnested
  WHERE NOT (selected_result = 'tab_to_search' AND engagement_type in ('click', 'enter'))
  AND NOT (selected_result = 'tip_dismissal_acknowledgement' AND engagement_type in ('click', 'enter'))
  AND NOT (engagement_type in ('dismiss', 'inaccurate_location', 'not_interested', 'not_relevant', 'show_less_frequently'))
)
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
      WHEN search_session_type IN ('engaged', 'annoyance')
        THEN selected_result
      ELSE NULL
    END AS engaged_result_type,
    CASE
      WHEN search_session_type IN ('engaged', 'annoyance')
        THEN product_selected_result
      ELSE NULL
    END AS product_engaged_result_type,
    result_type,
    product_result_type,
    num_chars_typed,
    num_total_results,
    CASE
      WHEN search_session_type = 'annoyance'
        THEN engagement_type
      ELSE NULL
    END AS annoyance_signal_type,
    COUNTIF(
      res = 'default_partner_search_suggestion'
    ) AS num_default_partner_search_suggestion_impressions,
    COUNTIF(res = 'search_engine_suggestion') AS num_search_engine_suggestion_impressions,
    COUNTIF(res = 'trending_suggestion') AS num_trending_suggestion_impressions,
    COUNTIF(res = 'history') AS num_history_impressions,
    COUNTIF(res = 'bookmark') AS num_bookmark_impressions,
    COUNTIF(res = 'open_tabs') AS num_open_tab_impressions,
    COUNTIF(res = 'admarketplace_sponsored') AS num_admarketplace_sponsored_impressions,
    COUNTIF(res = 'navigational') AS num_navigational_impressions,
    COUNTIF(res = 'add_on') AS num_add_on_impressions,
    COUNTIF(res = 'wikipedia_enhanced') AS num_wikipedia_enhanced_impressions,
    COUNTIF(res = 'wikipedia_dynamic') AS num_wikipedia_dynamic_impressions,
    COUNTIF(res = 'weather') AS num_weather_impressions,
    COUNTIF(res = 'quick_action') AS num_quick_action_impressions,
    COUNTIF(res = 'pocket_collection') AS num_pocket_collection_impressions,
    legacy_telemetry_client_id,
    client_id AS glean_metrics_client_id,
    ARRAY_CONCAT_AGG(experiments) AS experiments
  FROM
    intermediate_states_removed,
    UNNEST(product_results) AS res
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
    product_result_type,
    engaged_result_type,
    product_engaged_result_type,
    engagement_type,
    search_session_type,
    num_chars_typed,
    num_total_results
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
  product_engaged_result_type,
  annoyance_signal_type,
  result_type AS first_result_type,
  product_result_type AS product_first_result_type,
  num_chars_typed,
  num_total_results,
  num_default_partner_search_suggestion_impressions,
  num_search_engine_suggestion_impressions,
  num_trending_suggestion_impressions,
  num_history_impressions,
  num_open_tab_impressions,
  num_bookmark_impressions,
  num_admarketplace_sponsored_impressions,
  num_navigational_impressions,
  num_add_on_impressions,
  num_wikipedia_enhanced_impressions,
  num_wikipedia_dynamic_impressions,
  num_weather_impressions,
  num_quick_action_impressions,
  num_pocket_collection_impressions,
  experiments
FROM
  events_summary
