CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily_v6`
AS
SELECT
  submission_date AS submission_date_s3,
  * EXCEPT (
    active_experiment_id,
    active_experiment_branch,
    total_hours_sum,
    scalar_parent_dom_contentprocess_troubled_due_to_memory_sum,
    histogram_parent_devtools_developertoolbar_opened_count_sum
  ) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    IFNULL(geo_subdivision1, '??') AS geo_subdivision1,
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_sum
    ) AS scalar_parent_urlbar_picked_autofill_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_about_sum
    ) AS scalar_parent_urlbar_picked_autofill_about_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_adaptive_sum
    ) AS scalar_parent_urlbar_picked_autofill_adaptive_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_origin_sum
    ) AS scalar_parent_urlbar_picked_autofill_origin_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_other_sum
    ) AS scalar_parent_urlbar_picked_autofill_other_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_preloaded_sum
    ) AS scalar_parent_urlbar_picked_autofill_preloaded_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_autofill_url_sum
    ) AS scalar_parent_urlbar_picked_autofill_url_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_bookmark_sum
    ) AS scalar_parent_urlbar_picked_bookmark_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_dynamic_sum
    ) AS scalar_parent_urlbar_picked_dynamic_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_extension_sum
    ) AS scalar_parent_urlbar_picked_extension_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_formhistory_sum
    ) AS scalar_parent_urlbar_picked_formhistory_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_history_sum
    ) AS scalar_parent_urlbar_picked_history_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_keyword_sum
    ) AS scalar_parent_urlbar_picked_keyword_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_remotetab_sum
    ) AS scalar_parent_urlbar_picked_remotetab_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_searchengine_sum
    ) AS scalar_parent_urlbar_picked_searchengine_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_searchsuggestion_sum
    ) AS scalar_parent_urlbar_picked_searchsuggestion_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_switchtab_sum
    ) AS scalar_parent_urlbar_picked_switchtab_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_tabtosearch_sum
    ) AS scalar_parent_urlbar_picked_tabtosearch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_tip_sum
    ) AS scalar_parent_urlbar_picked_tip_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_topsite_sum
    ) AS scalar_parent_urlbar_picked_topsite_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_unknown_sum
    ) AS scalar_parent_urlbar_picked_unknown_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      scalar_parent_urlbar_picked_visiturl_sum
    ) AS scalar_parent_urlbar_picked_visiturl_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_dynamic_wikipedia_sum
    ) AS contextual_services_quicksuggest_block_dynamic_wikipedia_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_nonsponsored_sum
    ) AS contextual_services_quicksuggest_block_nonsponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_nonsponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_block_nonsponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_sponsored_sum
    ) AS contextual_services_quicksuggest_block_sponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_sponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_block_sponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_block_weather_sum
    ) AS contextual_services_quicksuggest_block_weather_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_dynamic_wikipedia_sum
    ) AS contextual_services_quicksuggest_click_dynamic_wikipedia_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_nonsponsored_sum
    ) AS contextual_services_quicksuggest_click_nonsponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_nonsponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_click_nonsponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_sponsored_sum
    ) AS contextual_services_quicksuggest_click_sponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_sponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_click_sponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_click_weather_sum
    ) AS contextual_services_quicksuggest_click_weather_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_dynamic_wikipedia_sum
    ) AS contextual_services_quicksuggest_help_dynamic_wikipedia_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_nonsponsored_sum
    ) AS contextual_services_quicksuggest_help_nonsponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_nonsponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_help_nonsponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_sponsored_sum
    ) AS contextual_services_quicksuggest_help_sponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_sponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_help_sponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_help_weather_sum
    ) AS contextual_services_quicksuggest_help_weather_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_dynamic_wikipedia_sum
    ) AS contextual_services_quicksuggest_impression_dynamic_wikipedia_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_nonsponsored_sum
    ) AS contextual_services_quicksuggest_impression_nonsponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_nonsponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_impression_nonsponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_sponsored_sum
    ) AS contextual_services_quicksuggest_impression_sponsored_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_sponsored_bestmatch_sum
    ) AS contextual_services_quicksuggest_impression_sponsored_bestmatch_sum,
    `mozfun.map.extract_keyed_scalar_sum`(
      contextual_services_quicksuggest_impression_weather_sum
    ) AS contextual_services_quicksuggest_impression_weather_sum,
    (
      SELECT
        SUM(value)
      FROM
        UNNEST(contextual_services_topsites_click_sum)
      WHERE
        key LIKE "newtab%"
    ) AS contextual_services_topsites_click_sum,
    (
      SELECT
        SUM(value)
      FROM
        UNNEST(contextual_services_topsites_impression_sum)
      WHERE
        key LIKE "newtab%"
    ) AS contextual_services_topsites_impression_sum
  ),
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
  COALESCE(
    scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
    scalar_parent_browser_engagement_total_uri_count_sum
  ) AS total_uri_count,
  scalar_parent_browser_engagement_total_uri_count_sum AS total_uri_count_normal_mode,
  -- Private Mode URI counts released in version 84
  -- https://sql.telemetry.mozilla.org/queries/87736/source#217392
  -- NOTE: These are replicated in clients_last_seen_v1, until we change that
  -- to read from views these will need to be updated there as well
  IF(
    mozfun.norm.extract_version(app_display_version, 'major') < 84,
    NULL,
    scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum - COALESCE(
      scalar_parent_browser_engagement_total_uri_count_sum,
      0
    )
  ) AS total_uri_count_private_mode,
  (
    SELECT
      LOGICAL_OR(mozfun.addons.is_adblocker(addon_id))
    FROM
      UNNEST(active_addons)
  ) AS has_adblocker_enabled,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_joined_v1`
