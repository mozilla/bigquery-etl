CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
AS
--Fennec Nightly
SELECT
  DATE(submission_timestamp) AS submission_date,
  * REPLACE ('nightly' AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.migration_v1`
UNION ALL
--Fennec Beta
SELECT
  DATE(submission_timestamp) AS submission_date,
  * REPLACE ('beta' AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.migration_v1`
UNION ALL
--Fennec Release
SELECT
  DATE(submission_timestamp) AS submission_date,
  * REPLACE ('release' AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.migration_v1`
