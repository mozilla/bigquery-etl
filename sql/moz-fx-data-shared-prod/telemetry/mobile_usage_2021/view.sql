CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_usage_2021`
AS
WITH base AS (
  SELECT
    *,
    -- This is a shim to get an os field based on the existing normalized
    -- `product` field in the underlying table. We will likely need to overhaul
    -- the mobile ETL in H1 2021 to provide a Glean-first approach, in which
    -- case shims like this will no longer be necessary.
    IF(product LIKE '% iOS', 'iOS', 'Android') AS os,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.mobile_usage_v1`
),
with_pinfo AS (
  SELECT
    *,
    mozfun.norm.product_info(product, os) AS pinfo,
  FROM
    base
)
-- Names here should be consistent with the desktop_usage_2021 view.
SELECT
  submission_date,
  COALESCE(cdou, 0) AS cdou,
  COALESCE(wau, 0) AS wau,
  COALESCE(mau, 0) AS mau,
  COALESCE(dau, 0) AS dau,
  id_bucket,
  pinfo.app_name,
  pinfo.canonical_app_name,
  normalized_channel AS channel,
  os,
  campaign,
  country,
  cc.name AS country_name,
  distribution_id
FROM
  with_pinfo AS usage
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS cc
  ON (usage.country = cc.code)
