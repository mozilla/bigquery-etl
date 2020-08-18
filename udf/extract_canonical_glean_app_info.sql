/*

Returns canonical, human-understandable identification info for Glean sources.

Currently supports only Fenix variants, and it should be expected that the output
of this function evolves over time. If we rename products, we may choose to update
the values here so that analyses consistently get the new name.

The first argument (app_id) can be fairly fuzzy; it is tolerant of actual Google
Play Store appId values like 'org.mozilla.firefox_beta' (mix of periods and underscores)
as well as BigQuery dataset names with suffixes like 'org_mozilla_firefox_beta_stable'.

The second argument (app_build_id) should be the value in client_info.app_build.

For more context on the history of naming for Mozilla's mobile browsers, see:
https://docs.google.com/spreadsheets/d/18PzkzZxdpFl23__-CIO735NumYDqu7jHpqllo0sBbPA/edit#gid=0

*/
CREATE OR REPLACE FUNCTION udf.extract_canonical_glean_app_info(app_id STRING, app_build_id STRING)
RETURNS STRUCT<app_name STRING, channel STRING, canonical_app_id STRING> AS (
  CASE
  -- Note that order is important here; we move from more specific names to more general
  -- so that we can properly ignore dataset suffixes like _stable or _live or _derived.
  WHEN
    app_id LIKE 'org_mozilla_fennec_aurora%'
  THEN
    STRUCT('Fenix', 'nightly', 'org.mozilla.fennec_aurora')
  WHEN
    app_id LIKE 'org_mozilla_firefox_beta%'
  THEN
    STRUCT('Fenix', 'beta', 'org.mozilla.firefox_beta')
  WHEN
    app_id LIKE 'org_mozilla_firefox%'
  THEN
    STRUCT('Fenix', 'release', 'org.mozilla.firefox')
  WHEN
    app_id LIKE 'org_mozilla_fenix_nightly%'
  THEN
    STRUCT('Firefox Preview', 'nightly', 'org.mozilla.fenix.nightly')
  WHEN
    app_id LIKE 'org_mozilla_fenix%'
  THEN
    IF(
      udf.fenix_build_to_datetime(app_build_id) < '2020-07-03',
      STRUCT('Firefox Preview', 'beta', 'org.mozilla.fenix'),
      STRUCT('Fenix', 'nightly', 'org.mozilla.fenix')
    )
  ELSE
    ERROR(
      FORMAT("Given app_id or dataset name does not match any known Glean application: %s", app_id)
    )
  END
);

-- Tests
SELECT
  assert_equals(
    STRUCT(
      'Firefox Preview' AS app_name,
      'beta' AS channel,
      'org.mozilla.fenix' AS canonical_app_id
    ),
    udf.extract_canonical_glean_app_info('org.mozilla.fenix_beta', '2015718419')
  ),
  assert_equals(
    STRUCT(
      'Firefox Preview' AS app_name,
      'beta' AS channel,
      'org.mozilla.fenix' AS canonical_app_id
    ),
    udf.extract_canonical_glean_app_info('org_mozilla_fenix_derived', '2015718419')
  ),
  assert_equals(
    STRUCT('Fenix' AS app_name, 'nightly' AS channel, 'org.mozilla.fenix' AS canonical_app_id),
    udf.extract_canonical_glean_app_info('org_mozilla_fenix_live', '3015718419')
  );
