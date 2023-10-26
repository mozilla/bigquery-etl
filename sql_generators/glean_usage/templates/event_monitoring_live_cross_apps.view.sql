CREATE OR REPLACE VIEW `{{ project_id }}.{{ dataset }}.event_monitoring_live_v1`
AS
{% for app in apps %}
SELECT
  * 
FROM 
  `{{ project_id }}.{{ app["bq_dataset_family"] }}.event_monitoring_live`
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
