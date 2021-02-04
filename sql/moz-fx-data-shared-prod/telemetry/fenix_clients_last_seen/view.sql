CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fenix_clients_last_seen`
AS
-- For context on naming and channels of Fenix apps, see:
-- https://docs.google.com/document/d/1Ym4eZyS0WngEP6WdwJjmCoxtoQbJSvORxlQwZpuSV2I/edit#heading=h.69hvvg35j8un
WITH fenix_union AS (
  SELECT
    *,
    'org_mozilla_fenix' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_fenix_nightly' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_firefox' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_firefox_beta' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline_clients_last_seen`
  UNION ALL
  SELECT
    *,
    'org_mozilla_fennec_aurora' AS _dataset
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline_clients_last_seen`
),
fenix_app_info AS (
  SELECT
    *,
    mozfun.norm.fenix_app_info(_dataset, app_build) AS _app_info
  FROM
    fenix_union
)
SELECT
  * EXCEPT (_dataset, _app_info) REPLACE(_app_info.channel AS normalized_channel),
  _app_info.app_name,
FROM
  fenix_app_info
