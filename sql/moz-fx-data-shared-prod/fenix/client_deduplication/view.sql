CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.client_deduplication`
AS
WITH unioned AS (
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
    `moz-fx-data-shared-prod.org_mozilla_fenix.client_deduplication`
)
SELECT
  client_info.client_id,
  COALESCE(
    metrics.string.client_deduplication_hashed_gaid,
    metrics.string.activation_identifier
  ) AS hashed_ad_id,
  *
FROM
  unioned
