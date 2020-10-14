/*

The case statement below can be generated based on the markdown table
in metadata.yaml via the query in generate_body.sql

*/
CREATE OR REPLACE FUNCTION norm.product_info(app_name STRING, os STRING)
RETURNS STRUCT<
  product STRING,
  canonical_name STRING,
  contributes_to_2019_kpi BOOLEAN,
  contributes_to_2020_kpi BOOLEAN
> AS (
  CASE
  WHEN
    app_name LIKE "Firefox"
    AND norm.os(os) LIKE "%"
  THEN
    STRUCT(
      "Firefox" AS product,
      "Firefox for Desktop" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Fenix"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Fenix" AS product,
      "Firefox for Android (Fenix)" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Fennec"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Fennec" AS product,
      "Firefox for Android (Fennec)" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Preview"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Preview" AS product,
      "Firefox Preview for Android" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Fennec"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Firefox iOS" AS product,
      "Firefox for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "FirefoxForFireTV"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Fire TV" AS product,
      "Firefox for Fire TV" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "FirefoxConnect"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Echo" AS product,
      "Firefox for Echo Show" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Zerda"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Lite" AS product,
      "Firefox Lite" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Zerda_cn"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Lite CN" AS product,
      "Firefox Lite (China)" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Focus"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Focus Android" AS product,
      "Firefox Focus for Android" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Focus"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Focus iOS" AS product,
      "Firefox Focus for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Klar"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Klar Android" AS product,
      "Firefox Klar for Android" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Klar"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Klar iOS" AS product,
      "Firefox Klar for iOS" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Lockbox"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Lockwise Android" AS product,
      "Lockwise for Android" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Lockbox"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Lockwise iOS" AS product,
      "Lockwise for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "FirefoxReality%"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Reality" AS product,
      "Firefox Reality" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox iOS"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Firefox iOS" AS product,
      "Firefox for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Fire TV"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Fire TV" AS product,
      "Firefox for Fire TV" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Echo"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Echo" AS product,
      "Firefox for Echo Show" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Lite"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Lite" AS product,
      "Firefox Lite" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Lite CN"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Lite CN" AS product,
      "Firefox Lite (China)" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Focus Android"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Focus Android" AS product,
      "Firefox Focus for Android" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Focus iOS"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Focus iOS" AS product,
      "Firefox Focus for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Klar Android"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Klar Android" AS product,
      "Firefox Klar for Android" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Klar iOS"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Klar iOS" AS product,
      "Firefox Klar for iOS" AS canonical_name,
      FALSE AS contributes_to_2019_kpi,
      FALSE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Lockwise Android"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Lockwise Android" AS product,
      "Lockwise for Android" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Lockwise iOS"
    AND norm.os(os) LIKE "iOS"
  THEN
    STRUCT(
      "Lockwise iOS" AS product,
      "Lockwise for iOS" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  WHEN
    app_name LIKE "Firefox Reality"
    AND norm.os(os) LIKE "Android"
  THEN
    STRUCT(
      "Firefox Reality" AS product,
      "Firefox Reality" AS canonical_name,
      TRUE AS contributes_to_2019_kpi,
      TRUE AS contributes_to_2020_kpi
    )
  ELSE
    ('Other', 'Other', FALSE, FALSE)
  END
);

-- Tests
SELECT
  assert.equals(
    STRUCT('Firefox', 'Firefox for Desktop', TRUE, TRUE),
    norm.product_info('Firefox', 'Windows')
  ),
  assert.equals(
    STRUCT('Fenix', 'Firefox for Android (Fenix)', TRUE, TRUE),
    norm.product_info('Fenix', 'Android')
  ),
  assert.equals(STRUCT('Other', 'Other', FALSE, FALSE), norm.product_info('Fenix', 'iOS')),
  assert.equals(
    STRUCT('Klar iOS', 'Firefox Klar for iOS', FALSE, FALSE),
    norm.product_info('Klar', 'iOS')
  ),
  assert.equals(
    STRUCT('Lockwise iOS', 'Lockwise for iOS', TRUE, TRUE),
    norm.product_info('Lockbox', 'iOS')
  ),
  -- Make sure os normalization works.
  assert.equals(
    STRUCT('Firefox iOS', 'Firefox for iOS', TRUE, TRUE),
    norm.product_info('Fennec', 'iPhone OS')
  ),
  -- Make sure we can pass in product values for app_name.
  assert.equals(
    STRUCT('Firefox iOS', 'Firefox for iOS', TRUE, TRUE),
    norm.product_info('Firefox iOS', 'iOS')
  );
