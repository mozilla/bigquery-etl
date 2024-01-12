CREATE OR REPLACE FUNCTION norm.fenix_app_info(app_id STRING, app_build_id STRING)
RETURNS STRUCT<app_name STRING, channel STRING, app_id STRING> AS (
  CASE
    -- Note that order is important here; we move from more specific names to more general
    -- so that we can properly ignore dataset suffixes like _stable or _live or _derived.
    WHEN app_id LIKE r'org\_mozilla\_fennec\_aurora%'
      THEN STRUCT('Fenix', 'nightly', 'org.mozilla.fennec_aurora')
    WHEN app_id LIKE r'org\_mozilla\_firefox\_beta%'
      THEN STRUCT('Fenix', 'beta', 'org.mozilla.firefox_beta')
    WHEN app_id LIKE r'org\_mozilla\_firefox%'
      THEN STRUCT('Fenix', 'release', 'org.mozilla.firefox')
    WHEN app_id LIKE r'org\_mozilla\_fenix\_nightly%'
      THEN STRUCT('Firefox Preview', 'nightly', 'org.mozilla.fenix.nightly')
    WHEN app_id LIKE r'org\_mozilla\_fenix%'
      THEN IF(
          -- See udf.fenix_build_to_datetime for info on build_id format;
          -- the build_id constant here corresponds to 2020-07-03;
          -- See also https://github.com/mozilla-mobile/fenix/issues/14031
          -- which describes a new 10-digit format for newer data.
          -- Until we have agreement with Fenix engineers on the long-term
          -- format, we do a simple cast and compare to an integer constant
          -- which should tolerate both formats.
          SAFE_CAST(app_build_id AS INT64) < 21850000,
          STRUCT('Firefox Preview', 'beta', 'org.mozilla.fenix'),
          STRUCT('Fenix', 'nightly', 'org.mozilla.fenix')
        )
    ELSE ERROR(
        FORMAT("Given app_id or dataset name does not match any known Fenix variant: %s", app_id)
      )
  END
);

-- Tests
SELECT
  assert.equals(
    STRUCT('Fenix' AS app_name, 'nightly' AS channel, 'org.mozilla.fennec_aurora' AS app_id),
    norm.fenix_app_info('org_mozilla_fennec_aurora_stable', '2015718419')
  ),
  assert.equals(
    STRUCT(
      'Firefox Preview' AS app_name,
      'nightly' AS channel,
      'org.mozilla.fenix.nightly' AS app_id
    ),
    norm.fenix_app_info('org.mozilla.fenix.nightly', '2015718419')
  );

WITH build_id AS (
  SELECT AS VALUE
    *
  FROM
    UNNEST(['21930609', '2015718419', '3015718419'])
)
SELECT
  assert.equals(
    STRUCT('Fenix' AS app_name, 'nightly' AS channel, 'org.mozilla.fenix' AS app_id),
    norm.fenix_app_info('org_mozilla_fenix_live', build_id)
  )
FROM
  build_id;

WITH build_id AS (
  SELECT AS VALUE
    *
  FROM
    UNNEST(['21730609', '11730609', '5'])
)
SELECT
  assert.equals(
    STRUCT('Firefox Preview' AS app_name, 'beta' AS channel, 'org.mozilla.fenix' AS app_id),
    norm.fenix_app_info('org_mozilla_fenix_live', build_id)
  )
FROM
  build_id;
