CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.client_deduplication`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.client_deduplication`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.client_deduplication`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.client_deduplication`
