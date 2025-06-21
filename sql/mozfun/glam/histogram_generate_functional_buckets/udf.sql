-- udf_functional_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_functional_buckets(
  log_base INT64,
  buckets_per_magnitude INT64,
  range_max INT64
)
RETURNS ARRAY<FLOAT64> AS (
  (
    WITH bucket_indexes AS (
      -- Generate all bucket indexes
      -- https://github.com/mozilla/glean/blob/main/glean-core/src/histogram/functional.rs
      SELECT
        GENERATE_ARRAY(0, CEIL(LOG(range_max + 1, log_base) * buckets_per_magnitude)) AS indexes
    ),
    buckets AS (
      SELECT
        FLOOR(POW(log_base, (idx) / buckets_per_magnitude)) AS bucket
      FROM
        bucket_indexes,
        UNNEST(indexes) AS idx
      WHERE
        FLOOR(POW(log_base, (idx) / buckets_per_magnitude)) <= range_max
    )
    SELECT
      ARRAY_CONCAT([0.0], ARRAY_AGG(DISTINCT(bucket) ORDER BY bucket))
    FROM
      buckets
  )
);

SELECT
  -- First 50 keys of a timing distribution
  -- https://mozilla.github.io/glean/book/user/metrics/timing_distribution.html
  -- https://sql.telemetry.mozilla.org/queries/75805/source
  assert.array_equals(
    [
      --format:off
      0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,19,20,22,24,26,29,32,34,38,41,
      45,49,53,58,64,69,76,82,90,98,107,117,128,139,152,165,181,197,215,234,
      256,279,304
      --format:on
    ],
    glam.histogram_generate_functional_buckets(2, 8, 305)
  ),
  -- Check for existence of first 10 keys of a memory distribution
  -- https://mozilla.github.io/glean/book/user/metrics/memory_distribution.html
  -- https://sql.telemetry.mozilla.org/queries/75805/source
  assert.equals(
    10,
    (
      SELECT
        COUNT(*)
      FROM
        UNNEST(
          [440871, 460390, 571740, 597053, 623487, 651091, 679917, 710019, 741455, 774282]
        ) expect
      JOIN
        UNNEST(glam.histogram_generate_functional_buckets(2, 16, 774282 + 1)) actual
      WHERE
        expect = actual
    )
  )
