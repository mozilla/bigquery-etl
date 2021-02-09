CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.first_session`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.first_session_v1`
