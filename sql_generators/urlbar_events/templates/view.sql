--- User-facing view. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.urlbar_events`
AS
SELECT
  submission_date,
  glean_client_id,
  legacy_telemetry_client_id,
  sample_id,
  event_name,
  event_timestamp,
  event_id,
  experiments,
  seq,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  os_version,
  normalized_engine,
  app_display_version,
  pref_ohttp_available,
  pref_ohttp_enabled,
  sap,
  pref_data_collection,
  pref_sponsored_suggestions,
  pref_fx_suggestions,
  engagement_type,
  interaction,
  num_chars_typed,
  num_total_results,
  selected_position,
  selected_result,
  results,
  product_selected_result,
  event_action,
  is_terminal,
  engaged_result_type,
  product_engaged_result_type,
  annoyance_signal_type,
  profile_group_id,
  window_mode
FROM
  `{{ project_id }}.{{ app_name }}_derived.urlbar_events_v2`
