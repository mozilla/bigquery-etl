/*

Accepts a glean metrics struct as input and returns a modified struct that
nulls out histograms for older versions of the Glean SDK that reported
pathological binning; see https://bugzilla.mozilla.org/show_bug.cgi?id=1592930

*/
CREATE OR REPLACE FUNCTION udf.normalize_fenix_metrics(
  telemetry_sdk_build STRING,
  metrics ANY TYPE
) AS (
  (
    SELECT AS STRUCT
      metrics.* REPLACE (
        IF(
          SAFE_CAST(SPLIT(telemetry_sdk_build, '.')[SAFE_OFFSET(0)] AS INT64) < 19,
          NULL,
          metrics.timing_distribution
        ) AS timing_distribution
      )
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    'foo',
    udf.normalize_fenix_metrics(
      '19.0.0',
      STRUCT(STRUCT('foo' AS foo) AS timing_distribution)
    ).timing_distribution.foo
  ),
  mozfun.assert.null(
    udf.normalize_fenix_metrics(
      '0.3.0',
      STRUCT(STRUCT('foo' AS foo) AS timing_distribution)
    ).timing_distribution.foo
  );
