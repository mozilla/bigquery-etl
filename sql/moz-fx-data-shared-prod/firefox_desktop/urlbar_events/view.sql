--- User-facing view. Generated via sql_generators.urlbar_events.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.urlbar_events`
AS
SELECT
  * EXCEPT (product_engaged_result_type, engaged_result_type, annoyance_signal_type)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.urlbar_events_v2`
