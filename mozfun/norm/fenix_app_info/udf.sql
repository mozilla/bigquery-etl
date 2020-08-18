CREATE OR REPLACE FUNCTION norm.fenix_app_info(app_id STRING, app_build_id STRING)
RETURNS STRUCT<app_name STRING, channel STRING, app_id STRING> AS (
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
      -- See udf.fenix_build_to_datetime for info on build_id format;
      -- the build_id constant here corresponds to 2020-07-03
      app_build_id < '21850000',
      STRUCT('Firefox Preview', 'beta', 'org.mozilla.fenix'),
      STRUCT('Fenix', 'nightly', 'org.mozilla.fenix')
    )
  ELSE
    ERROR(FORMAT("Given app_id or dataset name does not match any known Fenix variant: %s", app_id))
  END
);

-- Tests
SELECT
  assert_equals(
    STRUCT('Firefox Preview' AS app_name, 'beta' AS channel, 'org.mozilla.fenix' AS app_id),
    norm.fenix_app_info('org.mozilla.fenix_beta', '2015718419')
  ),
  assert_equals(
    STRUCT('Firefox Preview' AS app_name, 'beta' AS channel, 'org.mozilla.fenix' AS app_id),
    norm.fenix_app_info('org_mozilla_fenix_derived', '2015718419')
  ),
  assert_equals(
    STRUCT('Fenix' AS app_name, 'nightly' AS channel, 'org.mozilla.fenix' AS app_id),
    norm.fenix_app_info('org_mozilla_fenix_live', '3015718419')
  );
