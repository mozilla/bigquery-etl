--- User-facing view. Generated via sql_generators.urlbar_events.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.urlbar_events`
AS
SELECT
  * except (product_engaged_result_type, engaged_result_type, annoyance_signal_type)
FROM
  `{{ project_id }}.{{ app_name }}_derived.urlbar_events_v2`
