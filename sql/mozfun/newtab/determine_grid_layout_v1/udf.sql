-- newtab.determine_grid_layout_v1 figures out the layout typefor the tiles on the Newtab homepage
CREATE OR REPLACE FUNCTION newtab.determine_grid_layout_v1(
  is_section BOOLEAN,
  app_version INTEGER,
  experiments ARRAY<
    STRUCT<key STRING, value STRUCT<branch STRING, extra STRUCT<type STRING, enrollment_id STRING>>>
  >
)
RETURNS STRING AS (
  CASE
    WHEN is_section
      THEN 'SECTION_GRID'
    WHEN app_version >= 136
      THEN 'NEW_GRID'
    WHEN EXISTS(
        SELECT
          1
        FROM
          UNNEST(experiments) AS ex
        WHERE
          ex.key IN (
            'default-ui-experiment',
            'new-tab-layout-variant-b-and-content-card-ui-rollout-global',
            'new-tab-layout-variant-b-and-content-card-ui-release-rollout-global-v2',
            'default-ui-experiment-logo-in-corner-rollout'
          )
      )
      THEN 'NEW_GRID'
    ELSE 'OLD_GRID'
  END
);

-- Tests
WITH new_grid AS (
  SELECT
    [
      STRUCT(
        'default-ui-experiment' AS key,
        STRUCT(
          'experiment-name' AS branch,
          STRUCT('nimbus-nimbus' AS type, 'n/a' AS enrollment_id) AS extra
        ) AS value
      )
    ] AS experiments
),
old_grid AS (
  SELECT
    [
      STRUCT(
        'some-old-experiment' AS key,
        STRUCT(
          'experiment-name' AS branch,
          STRUCT('nimbus-nimbus' AS type, 'n/a' AS enrollment_id) AS extra
        ) AS value
      )
    ] AS experiments
)
SELECT
  newtab.determine_grid_layout_v1(FALSE, 130, new_grid.experiments),
  newtab.determine_grid_layout_v1(FALSE, 130, old_grid.experiments),
  newtab.determine_grid_layout_v1(FALSE, 136, old_grid.experiments),
  newtab.determine_grid_layout_v1(TRUE, 136, old_grid.experiments),
FROM
  new_grid,
  old_grid;
