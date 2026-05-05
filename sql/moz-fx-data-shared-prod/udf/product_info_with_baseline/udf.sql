CREATE OR REPLACE FUNCTION udf.product_info_with_baseline(
  legacy_app_name STRING,
  normalized_os STRING
)
RETURNS STRUCT<
  app_name STRING,
  product STRING,
  canonical_app_name STRING,
  canonical_name STRING,
  contributes_to_2019_kpi BOOLEAN,
  contributes_to_2020_kpi BOOLEAN,
  contributes_to_2021_kpi BOOLEAN
> AS (
  CASE
    WHEN legacy_app_name LIKE "Focus iOS Baseline"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "focus_ios" AS app_name,
          "Focus iOS Baseline" AS product,
          "Focus iOS" AS canonical_app_name,
          "Focus iOS Baseline" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Focus Android Baseline"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "focus_android" AS app_name,
          "Focus Android Baseline" AS product,
          "Focus Android" AS canonical_app_name,
          "Focus Android Baseline" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar Android Baseline"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "klar_android" AS app_name,
          "Klar Android Baseline" AS product,
          "Klar Android" AS canonical_app_name,
          "Klar Android Baseline" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar iOS Baseline"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "klar_ios" AS app_name,
          "Klar iOS Baseline" AS product,
          "Klar iOS" AS canonical_app_name,
          "Klar iOS Baseline" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    ELSE mozfun.norm.product_info(legacy_app_name, normalized_os)
  END
);

-- Tests
WITH b AS (
  SELECT AS VALUE
    udf.product_info_with_baseline('Firefox', 'Windows')
)
SELECT
  mozfun.assert.equals('firefox_desktop', b.app_name),
  mozfun.assert.equals('Firefox', b.product),
  mozfun.assert.equals('Firefox for Desktop', b.canonical_app_name),
  mozfun.assert.equals('Firefox for Desktop', b.canonical_name),
  mozfun.assert.true(b.contributes_to_2020_kpi),
  mozfun.assert.true(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    udf.product_info_with_baseline('Focus iOS Baseline', 'iOS')
)
SELECT
  mozfun.assert.equals('focus_ios', b.app_name),
  mozfun.assert.equals('Focus iOS Baseline', b.product),
  mozfun.assert.equals('Focus iOS', b.canonical_app_name),
  mozfun.assert.true(b.contributes_to_2020_kpi),
  mozfun.assert.true(b.contributes_to_2021_kpi),
FROM
  b;
