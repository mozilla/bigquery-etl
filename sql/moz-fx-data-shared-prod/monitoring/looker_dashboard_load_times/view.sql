CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.looker_dashboard_load_times`
AS
SELECT
  total_runtime,
  seconds_until_controller_initialized,
  seconds_until_dom_content_loaded,
  seconds_until_metadata_loaded,
  seconds_until_dashboard_run_start,
  -- seconds_until_dashboard_run_start represents the time the tab was opened,
  -- so it needs to be used as offset to get the number of seconds
  seconds_until_first_data_received - seconds_until_dashboard_run_start AS seconds_until_first_data_received,
  -- tile rendering times should be set to 0, if data is loaded from cache
  -- if the rendering time is lower than the run start, then it indicates it's loaded from cache
  IF(
    seconds_until_first_tile_finished_rendering > seconds_until_dashboard_run_start,
    seconds_until_first_tile_finished_rendering - seconds_until_dashboard_run_start,
    0
  ) AS seconds_until_first_tile_finished_rendering,
  seconds_until_last_data_received - seconds_until_dashboard_run_start AS seconds_until_last_data_received,
  IF(
    seconds_until_last_tile_finished_rendering > seconds_until_dashboard_run_start,
    seconds_until_last_tile_finished_rendering - seconds_until_dashboard_run_start,
    0
  ) AS seconds_until_last_tile_finished_rendering,
  dash_id,
  dashboard_page_session,
  submission_date,
  refresh_interval,
  -- some of these numbers might not make sense for auto-refreshed dashboards, so providing this as filter option
  refresh_interval IS NOT NULL AS is_auto_refreshed,
  dashboard_run_session
FROM
  `moz-fx-data-shared-prod.monitoring_derived.looker_dashboard_load_times_v1`
