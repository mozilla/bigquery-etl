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
    WHEN app_version >= 155
      AND is_section
      THEN 'POSTNOVA_SECTION'
    WHEN app_version >= 155
      THEN 'POSTNOVA_GRID'
    WHEN app_version >= 151
      AND is_section
      THEN 'NOVA_SECTION'
    WHEN app_version >= 151
      THEN 'NOVA_GRID'
    WHEN is_section
      THEN 'SECTION_GRID'
    WHEN app_version >= 136
      THEN 'NEW_GRID'
    WHEN EXISTS (
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
  -- OLD_GRID: pre-136 with no qualifying experiment and is_section=FALSE
  assert.equals('OLD_GRID', newtab.determine_grid_layout_v1(FALSE, 130, old_grid.experiments)),
  -- NEW_GRID via experiment override (pre-136 but qualifying experiment)
  assert.equals('NEW_GRID', newtab.determine_grid_layout_v1(FALSE, 130, new_grid.experiments)),
  -- NEW_GRID via version >= 136 (regardless of experiment) up to but not including 151
  assert.equals('NEW_GRID', newtab.determine_grid_layout_v1(FALSE, 136, old_grid.experiments)),
  assert.equals('NEW_GRID', newtab.determine_grid_layout_v1(FALSE, 150, old_grid.experiments)),
  -- SECTION_GRID: is_section=TRUE at any pre-Nova version (< 151)
  assert.equals('SECTION_GRID', newtab.determine_grid_layout_v1(TRUE, 100, old_grid.experiments)),
  assert.equals('SECTION_GRID', newtab.determine_grid_layout_v1(TRUE, 136, old_grid.experiments)),
  assert.equals('SECTION_GRID', newtab.determine_grid_layout_v1(TRUE, 150, old_grid.experiments)),
  -- NOVA_GRID / NOVA_SECTION: version 151..154 (below the POSTNOVA cutoff of 155)
  assert.equals('NOVA_GRID', newtab.determine_grid_layout_v1(FALSE, 151, old_grid.experiments)),
  assert.equals('NOVA_GRID', newtab.determine_grid_layout_v1(FALSE, 154, old_grid.experiments)),
  assert.equals('NOVA_SECTION', newtab.determine_grid_layout_v1(TRUE, 151, old_grid.experiments)),
  assert.equals('NOVA_SECTION', newtab.determine_grid_layout_v1(TRUE, 154, old_grid.experiments)),
  -- POSTNOVA_GRID / POSTNOVA_SECTION: version >= 155
  assert.equals(
    'POSTNOVA_SECTION',
    newtab.determine_grid_layout_v1(TRUE, 155, old_grid.experiments)
  ),
  assert.equals('POSTNOVA_GRID', newtab.determine_grid_layout_v1(FALSE, 155, old_grid.experiments)),
  assert.equals(
    'POSTNOVA_SECTION',
    newtab.determine_grid_layout_v1(TRUE, 161, old_grid.experiments)
  ),
  assert.equals('POSTNOVA_GRID', newtab.determine_grid_layout_v1(FALSE, 161, old_grid.experiments)),
FROM
  new_grid,
  old_grid;
