CALL BQ.REFRESH_MATERIALIZED_VIEW(
  '{{ project_id }}.{{ derived_dataset }}.event_monitoring_live_v1'
);
