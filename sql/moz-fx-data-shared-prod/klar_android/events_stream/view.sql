-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.events_stream`
AS
SELECT
  "org_mozilla_klar" AS normalized_app_id,
  e.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar.events_stream` AS e
