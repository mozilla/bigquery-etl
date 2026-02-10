-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.events_first_seen`
AS
WITH events_first_seen_union AS (
  SELECT
    "org_mozilla_firefox" AS normalized_app_id,
    * REPLACE ("release" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_firefox_beta" AS normalized_app_id,
    * REPLACE ("beta" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_fenix" AS normalized_app_id,
    * REPLACE ("nightly" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_fenix_nightly" AS normalized_app_id,
    * REPLACE ("nightly" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.events_first_seen`
  UNION ALL
    BY NAME
  SELECT
    "org_mozilla_fennec_aurora" AS normalized_app_id,
    * REPLACE ("nightly" AS normalized_channel)
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.events_first_seen`
)
SELECT
  *
FROM
  events_first_seen_union
