WITH events AS (
  SELECT
    submission_date,
    client_id,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "block"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
    ) AS block_nonsponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "block"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
    ) AS block_sponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "block"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS block_nonsponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "block"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS block_sponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "click"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
    ) AS click_nonsponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "click"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
    ) AS click_sponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "click"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS click_nonsponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "click"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS click_sponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "help"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
    ) AS help_nonsponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "help"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
    ) AS help_sponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "help"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS help_nonsponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "help"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS help_sponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "impression_only"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
    ) AS impression_nonsponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "impression_only"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
    ) AS impression_sponsored_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "impression_only"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "nonsponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS impression_nonsponsored_bestmatch_count,
    COUNTIF(
      event_category = "contextservices.quicksuggest"
      AND event_method = "engagement"
      AND event_object = "impression_only"
      AND mozfun.map.get_key(event_map_values, 'suggestion_type') = "sponsored"
      AND mozfun.map.get_key(event_map_values, 'match_type') = "best-match"
    ) AS impression_sponsored_bestmatch_count,
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    submission_date = @submission_date
  GROUP BY
    1,
    2
),
clients AS (
  SELECT
    submission_date,
    client_id,
    COALESCE(
      user_pref_browser_urlbar_quicksuggest_data_collection_enabled = "true",
      FALSE
    ) AS user_pref_data_collection_enabled,
    COALESCE(
      user_pref_browser_urlbar_suggest_quicksuggest_sponsored = "true",
      FALSE
    ) AS user_pref_sponsored_suggestions_enabled,
    COALESCE(
      user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored = "true",
      FALSE
    ) AS user_pref_firefox_suggest_enabled,
    browser_version_info,
    country,
    experiments,
    locale,
    normalized_channel,
    normalized_os_version,
    sample_id,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = @submission_date
  --Suggest is only available for the following clients:
    AND country = "US"
    AND locale LIKE "en%"
    AND browser_version_info.major_version >= 92
    AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
)
SELECT
  *
FROM
  events
INNER JOIN
  clients
  USING (submission_date, client_id)
