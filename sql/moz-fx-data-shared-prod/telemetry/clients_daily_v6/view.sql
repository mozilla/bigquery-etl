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
    IFNULL(geo_subdivision2, '??') AS geo_subdivision2
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
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_joined_v1`
