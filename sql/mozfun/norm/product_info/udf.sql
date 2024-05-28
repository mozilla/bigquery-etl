/*

The case statement below can be generated based on the markdown table
in metadata.yaml via the query in generate_body.sql

*/
CREATE OR REPLACE FUNCTION norm.product_info(legacy_app_name STRING, normalized_os STRING)
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
    WHEN legacy_app_name LIKE "Firefox"
      AND normalized_os LIKE "%"
      THEN STRUCT(
          "firefox_desktop" AS app_name,
          "Firefox" AS product,
          "Firefox for Desktop" AS canonical_app_name,
          "Firefox for Desktop" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Fenix"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "fenix" AS app_name,
          "Fenix" AS product,
          "Firefox for Android (Fenix)" AS canonical_app_name,
          "Firefox for Android (Fenix)" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Fennec"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "fennec" AS app_name,
          "Fennec" AS product,
          "Firefox for Android (Fennec)" AS canonical_app_name,
          "Firefox for Android (Fennec)" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Preview"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_preview" AS app_name,
          "Firefox Preview" AS product,
          "Firefox Preview for Android" AS canonical_app_name,
          "Firefox Preview for Android" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Fennec"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "firefox_ios" AS app_name,
          "Firefox iOS" AS product,
          "Firefox for iOS" AS canonical_app_name,
          "Firefox for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "FirefoxForFireTV"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_fire_tv" AS app_name,
          "Firefox Fire TV" AS product,
          "Firefox for Fire TV" AS canonical_app_name,
          "Firefox for Fire TV" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "FirefoxConnect"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_connect" AS app_name,
          "Firefox Echo" AS product,
          "Firefox for Echo Show" AS canonical_app_name,
          "Firefox for Echo Show" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Zerda"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_lite" AS app_name,
          "Firefox Lite" AS product,
          "Firefox Lite" AS canonical_app_name,
          "Firefox Lite" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE r"Zerda\_cn"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_lite_cn" AS app_name,
          "Firefox Lite CN" AS product,
          "Firefox Lite (China)" AS canonical_app_name,
          "Firefox Lite (China)" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Focus"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "focus_android" AS app_name,
          "Focus Android" AS product,
          "Firefox Focus for Android" AS canonical_app_name,
          "Firefox Focus for Android" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Focus"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "focus_ios" AS app_name,
          "Focus iOS" AS product,
          "Firefox Focus for iOS" AS canonical_app_name,
          "Firefox Focus for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "klar_android" AS app_name,
          "Klar Android" AS product,
          "Firefox Klar for Android" AS canonical_app_name,
          "Firefox Klar for Android" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "klar_ios" AS app_name,
          "Klar iOS" AS product,
          "Firefox Klar for iOS" AS canonical_app_name,
          "Firefox Klar for iOS" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Lockbox"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "lockwise_android" AS app_name,
          "Lockwise Android" AS product,
          "Lockwise for Android" AS canonical_app_name,
          "Lockwise for Android" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Lockbox"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "lockwise_ios" AS app_name,
          "Lockwise iOS" AS product,
          "Lockwise for iOS" AS canonical_app_name,
          "Lockwise for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "FirefoxReality%"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_reality" AS app_name,
          "Firefox Reality" AS product,
          "Firefox Reality" AS canonical_app_name,
          "Firefox Reality" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox iOS"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "firefox_ios" AS app_name,
          "Firefox iOS" AS product,
          "Firefox for iOS" AS canonical_app_name,
          "Firefox for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Fire TV"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_fire_tv" AS app_name,
          "Firefox Fire TV" AS product,
          "Firefox for Fire TV" AS canonical_app_name,
          "Firefox for Fire TV" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Echo"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_connect" AS app_name,
          "Firefox Echo" AS product,
          "Firefox for Echo Show" AS canonical_app_name,
          "Firefox for Echo Show" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Lite"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_lite" AS app_name,
          "Firefox Lite" AS product,
          "Firefox Lite" AS canonical_app_name,
          "Firefox Lite" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Lite CN"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_lite_cn" AS app_name,
          "Firefox Lite CN" AS product,
          "Firefox Lite (China)" AS canonical_app_name,
          "Firefox Lite (China)" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Focus Android"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "focus_android" AS app_name,
          "Focus Android" AS product,
          "Firefox Focus for Android" AS canonical_app_name,
          "Firefox Focus for Android" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Focus iOS"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "focus_ios" AS app_name,
          "Focus iOS" AS product,
          "Firefox Focus for iOS" AS canonical_app_name,
          "Firefox Focus for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          TRUE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar Android"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "klar_android" AS app_name,
          "Klar Android" AS product,
          "Firefox Klar for Android" AS canonical_app_name,
          "Firefox Klar for Android" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Klar iOS"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "klar_ios" AS app_name,
          "Klar iOS" AS product,
          "Firefox Klar for iOS" AS canonical_app_name,
          "Firefox Klar for iOS" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Lockwise Android"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "lockwise_android" AS app_name,
          "Lockwise Android" AS product,
          "Lockwise for Android" AS canonical_app_name,
          "Lockwise for Android" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Lockwise iOS"
      AND normalized_os LIKE "iOS"
      THEN STRUCT(
          "lockwise_ios" AS app_name,
          "Lockwise iOS" AS product,
          "Lockwise for iOS" AS canonical_app_name,
          "Lockwise for iOS" AS canonical_name,
          TRUE AS contributes_to_2019_kpi,
          TRUE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    WHEN legacy_app_name LIKE "Firefox Reality"
      AND normalized_os LIKE "Android"
      THEN STRUCT(
          "firefox_reality" AS app_name,
          "Firefox Reality" AS product,
          "Firefox Reality" AS canonical_app_name,
          "Firefox Reality" AS canonical_name,
          FALSE AS contributes_to_2019_kpi,
          FALSE AS contributes_to_2020_kpi,
          FALSE AS contributes_to_2021_kpi
        )
    ELSE ('other', 'Other', 'Other', 'Other', FALSE, FALSE, FALSE)
  END
);

-- Tests
WITH b AS (
  SELECT AS VALUE
    norm.product_info('Firefox', 'Windows')
)
SELECT
  assert.equals('firefox_desktop', b.app_name),
  assert.equals('Firefox', b.product),
  assert.equals('Firefox for Desktop', b.canonical_app_name),
  assert.equals('Firefox for Desktop', b.canonical_name),
  assert.true(b.contributes_to_2020_kpi),
  assert.true(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    norm.product_info('Firefox iOS', 'iOS')
)
SELECT
  assert.equals('firefox_ios', b.app_name),
  assert.equals('Firefox iOS', b.product),
  assert.equals('Firefox for iOS', b.canonical_app_name),
  assert.true(b.contributes_to_2020_kpi),
  assert.true(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    norm.product_info('Lockbox', 'iOS')
)
SELECT
  assert.equals('lockwise_ios', b.app_name),
  assert.equals('Lockwise iOS', b.product),
  assert.equals('Lockwise for iOS', b.canonical_name),
  assert.equals('Lockwise for iOS', b.canonical_app_name),
  assert.true(b.contributes_to_2020_kpi),
  assert.false(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    norm.product_info('Klar', 'iOS')
)
SELECT
  assert.equals('klar_ios', b.app_name),
  assert.equals('Klar iOS', b.product),
  assert.equals('Firefox Klar for iOS', b.canonical_name),
  assert.equals('Firefox Klar for iOS', b.canonical_app_name),
  assert.false(b.contributes_to_2020_kpi),
  assert.false(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    norm.product_info('Fenix', 'Android')
)
SELECT
  assert.equals('fenix', b.app_name),
  assert.equals('Fenix', b.product),
  assert.equals('Firefox for Android (Fenix)', b.canonical_app_name),
  assert.true(b.contributes_to_2020_kpi),
  assert.true(b.contributes_to_2021_kpi),
FROM
  b;

WITH b AS (
  SELECT AS VALUE
    norm.product_info('foo', 'bar')
)
SELECT
  assert.equals('other', b.app_name),
  assert.equals('Other', b.product),
  assert.equals('Other', b.canonical_app_name),
  assert.false(b.contributes_to_2020_kpi),
  assert.false(b.contributes_to_2021_kpi),
FROM
  b;
