CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_usage_2021`
AS
WITH base AS (
  SELECT
    *,
    IF(product LIKE '% iOS', 'iOS', 'Android') AS os,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.mobile_usage_v1`
),
with_product_info AS (
  SELECT
    *,
    mozdata.analysis.klukas_product_info(product, os).*,
  FROM
    base
)
SELECT
  submission_date,
  cdou,
  mau,
  wau,
  dau,
  id_bucket,
  looker_app_name AS app_name,
  canonical_app_name,
  normalized_channel AS channel,
  os,
  campaign,
  country,
  distribution_id
FROM
  with_product_info
