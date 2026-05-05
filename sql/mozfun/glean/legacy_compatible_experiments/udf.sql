CREATE OR REPLACE FUNCTION glean.legacy_compatible_experiments(
  ping_info__experiments ARRAY<
    STRUCT<key STRING, value STRUCT<branch STRING, extra STRUCT<type STRING, enrollment_id STRING>>>
  >
)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  ARRAY(SELECT STRUCT(x.key AS key, x.value.branch AS value) FROM UNNEST(ping_info__experiments) x)
);

-- Tests
WITH ping_info AS (
  SELECT
    [
      STRUCT(
        "experiment_a" AS key,
        STRUCT(
          "control" AS branch,
          STRUCT("firefox" AS type, "123" AS enrollment_id) AS extra
        ) AS value
      ),
      STRUCT(
        "experiment_b" AS key,
        STRUCT(
          "treatment" AS branch,
          STRUCT("firefoxOS" AS type, "456" AS enrollment_id) AS extra
        ) AS value
      )
    ] AS experiments
),
expected AS (
  SELECT
    [
      STRUCT("experiment_a" AS key, "control" AS value),
      STRUCT("experiment_b" AS key, "treatment" AS value)
    ] AS experiments
)
SELECT
  assert.array_equals(
    expected.experiments,
    glean.legacy_compatible_experiments(ping_info.experiments)
  )
FROM
  ping_info,
  expected
