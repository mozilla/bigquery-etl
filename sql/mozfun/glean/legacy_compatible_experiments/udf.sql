CREATE OR REPLACE FUNCTION glean.legacy_compatible_experiments(ping_info__experiments array)
RETURNS array AS (
  array(select struct(x.key as key, x.value.branch as value) from unnest(ping_info__experiments) x) as experiments
);

-- Tests
with ping_info as (
  select array(
    struct("experiment_a" as key, struct("control" as branch, struct("type" as "firefox") as extra) as value),
    struct("experiment_b" as key, struct("treatment" as branch, struct("type" as "firefoxOS") as extra) as value),
  ) as experiments

),
expected as (
  select array(
    struct("experiment_a" as key, "control" as value),
    struct("experiment_b" as key, "treatment" as value),
  ) as experiments

),

)
SELECT
  assert.equals(expected, glean.legacy_compatible_experiments(ping_info.experiments))
