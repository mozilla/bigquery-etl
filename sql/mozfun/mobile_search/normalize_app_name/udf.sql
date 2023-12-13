

CREATE OR REPLACE FUNCTION `mobile_search.normalize_app_name`(app_name STRING, os STRING)
RETURNS STRUCT<normalized_app_name STRING, normalized_app_name_os STRING>
AS
(
  STRUCT(
    CASE
      WHEN app_name = 'Fenix' AND os = 'Android' THEN 'Firefox' 
      WHEN app_name = 'Fennec' AND os = 'Other' THEN 'Fennec' 
      WHEN app_name = 'Fennec' AND os = 'Android' THEN 'Fennec'
      WHEN app_name = 'Fennec' AND os = 'iOS' THEN 'Firefox'
      WHEN app_name = 'Firefox Preview' AND os = 'Android' THEN 'Firefox Preview'
      WHEN app_name = 'FirefoxConnect' AND os = 'Android' THEN 'Firefox for Echo Show'
      WHEN app_name = 'FirefoxForFireTV' AND os = 'Android' THEN 'Firefox for FireTV'
      WHEN app_name = 'Focus Android Glean' AND os = 'Android' THEN 'Focus'
      WHEN app_name = 'Focus iOS Glean' AND os = 'iOS' THEN 'Focus'
      WHEN app_name = 'Klar Android Glean' AND os = 'Android' THEN 'Klar'
      WHEN app_name = 'Klar iOS Glean' AND os = 'iOS' THEN 'Klar'
      WHEN app_name = 'Other' AND os = 'iOS' THEN 'Other'
      WHEN app_name = 'Other' AND os = 'Other' THEN 'Other'
      WHEN app_name = 'Other' AND os = 'Android' THEN 'Other'
      WHEN app_name = 'Zerda' AND os = 'Android' THEN 'Firefox Lite'
      WHEN app_name = 'Zerda_cn' AND os = 'Android' THEN 'Firefox Lite (China)'
      ELSE NULL
      END AS normalized_app_name,
    CASE
      WHEN app_name = 'Fenix' AND os = 'Android' THEN 'Firefox Android' 
      WHEN app_name = 'Fennec' AND os = 'Other' THEN 'Fennec Other' 
      WHEN app_name = 'Fennec' AND os = 'Android' THEN 'Legacy Firefox Android'
      WHEN app_name = 'Fennec' AND os = 'iOS' THEN 'Firefox iOS'
      WHEN app_name = 'Firefox Preview' AND os = 'Android' THEN 'Firefox Preview'
      WHEN app_name = 'FirefoxConnect' AND os = 'Android' THEN 'Firefox for Echo Show'
      WHEN app_name = 'FirefoxForFireTV' AND os = 'Android' THEN 'Firefox for FireTV'
      WHEN app_name = 'Focus Android Glean' AND os = 'Android' THEN 'Focus Android'
      WHEN app_name = 'Focus iOS Glean' AND os = 'iOS' THEN 'Focus iOS'
      WHEN app_name = 'Klar Android Glean' AND os = 'Android' THEN 'Klar Android'
      WHEN app_name = 'Klar iOS Glean' AND os = 'iOS' THEN 'Klar iOS'
      WHEN app_name = 'Other' AND os = 'iOS' THEN 'Other iOS'
      WHEN app_name = 'Other' AND os = 'Other' THEN 'Other'
      WHEN app_name = 'Other' AND os = 'Android' THEN 'Other Android'
      WHEN app_name = 'Zerda' AND os = 'Android' THEN 'Firefox Lite Android'
      WHEN app_name = 'Zerda_cn' AND os = 'Android' THEN 'Firefox Lite Android (China)'
      ELSE NULL
      END AS normalized_app_name_os
  )
);

-- Tests
SELECT
  assert.equals(
    STRUCT('Firefox' AS normalized_app_name, 'Firefox Android' AS normalized_app_name_os),
    mobile_search.normalize_app_name('Fenix', 'Android')
  ),
  assert.equals(
    STRUCT('Firefox Lite' AS normalized_app_name, 'Firefox Lite Android' AS normalized_app_name_os),
    mobile_search.normalize_app_name('Zerda', 'Android')
  );