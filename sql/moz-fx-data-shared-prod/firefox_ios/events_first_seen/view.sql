-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.events_first_seen`
AS
WITH events_first_seen_union AS (
  SELECT
    "org_mozilla_ios_firefox" AS normalized_app_id,
    * REPLACE ("release" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
    * REPLACE ("beta" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_ios_fennec" AS normalized_app_id,
    * REPLACE ("nightly" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec.events_first_seen`
)
SELECT
  *
FROM
  events_first_seen_union
