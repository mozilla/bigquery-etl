-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_fire_tv.events_stream`
AS
SELECT
  "org_mozilla_tv_firefox" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_tv_firefox.events_stream` AS e
