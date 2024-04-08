-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.events_stream`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  e.* REPLACE ("beta" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.events_stream` AS e
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  e.* REPLACE ("nightly" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.events_stream` AS e
