-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.events_stream`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  e.* REPLACE ("beta" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  e.* REPLACE ("nightly" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.events_stream` AS e
