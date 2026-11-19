CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.urlbar_events_daily`
AS
SELECT
  * EXCEPT (urlbar_sessions)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.urlbar_events_daily_v2`
