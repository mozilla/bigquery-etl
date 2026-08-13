-- newtab.determine_tiles_per_row_v2 figures out the number of tiles per row on the Newtab homepage layout
CREATE OR REPLACE FUNCTION newtab.determine_tiles_per_row_v2(
  layout_type STRING,
  newtab_window_inner_width INTEGER
)
RETURNS INTEGER AS (
  CASE
    WHEN newtab_window_inner_width IS NULL
      THEN NULL
    WHEN layout_type IN ('POSTNOVA_GRID', 'POSTNOVA_SECTION')
      THEN
        CASE
          WHEN newtab_window_inner_width < 724
            THEN 1
          WHEN newtab_window_inner_width < 1069
            THEN 2
          WHEN newtab_window_inner_width < 1414
            THEN 3
          ELSE 4
        END
    WHEN layout_type IN ('NOVA_SECTION', 'NOVA_GRID')
      THEN
        CASE
          WHEN newtab_window_inner_width < 1024
            THEN 1
          WHEN newtab_window_inner_width < 1366
            THEN 2
          WHEN newtab_window_inner_width < 1920
            THEN 3
          ELSE 4
        END
    WHEN layout_type = 'SECTION_GRID'
      THEN
        CASE
          WHEN newtab_window_inner_width < 724
            THEN 1
          WHEN newtab_window_inner_width < 1122
            THEN 2
          WHEN newtab_window_inner_width < 1390
            THEN 3
          ELSE 4
        END
    WHEN layout_type IN ('NEW_GRID', 'OLD_GRID')
      THEN
        CASE
          WHEN newtab_window_inner_width < 724
            THEN 1
          WHEN newtab_window_inner_width < 1122
            THEN 2
          WHEN newtab_window_inner_width < 1698
            OR layout_type = 'OLD_GRID'
            THEN 3
          ELSE 4
        END
    ELSE NULL
  END
);

-- Tests
SELECT
  -- NULL / unknown handling
  assert.equals(NULL, newtab.determine_tiles_per_row_v2('NEW_GRID', NULL)),
  assert.equals(NULL, newtab.determine_tiles_per_row_v2('UNKNOWN_LAYOUT', 1200)),
  -- POSTNOVA_GRID / POSTNOVA_SECTION: <724 / <1069 / <1414 / else
  assert.equals(1, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 723)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 724)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 1068)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 1069)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 1413)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 1414)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('POSTNOVA_GRID', 3000)),
  assert.equals(1, newtab.determine_tiles_per_row_v2('POSTNOVA_SECTION', 723)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('POSTNOVA_SECTION', 724)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('POSTNOVA_SECTION', 1069)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('POSTNOVA_SECTION', 1414)),
  -- NOVA_GRID / NOVA_SECTION: <1024 / <1366 / <1920 / else
  assert.equals(1, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1023)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1024)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1365)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1366)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1919)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('NOVA_GRID', 1920)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('NOVA_GRID', 3000)),
  assert.equals(1, newtab.determine_tiles_per_row_v2('NOVA_SECTION', 1023)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('NOVA_SECTION', 1024)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('NOVA_SECTION', 1366)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('NOVA_SECTION', 1920)),
  -- SECTION_GRID: <724 / <1122 / <1390 / else
  assert.equals(1, newtab.determine_tiles_per_row_v2('SECTION_GRID', 723)),
  assert.equals(2, newtab.determine_tiles_per_row_v2('SECTION_GRID', 724)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('SECTION_GRID', 1122)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('SECTION_GRID', 1390)),
  -- NEW_GRID / OLD_GRID: <724 / <1122 / (<1698 or OLD_GRID) / else (NEW_GRID only)
  assert.equals(3, newtab.determine_tiles_per_row_v2('OLD_GRID', 1698)),
  assert.equals(3, newtab.determine_tiles_per_row_v2('OLD_GRID', 3000)),
  assert.equals(4, newtab.determine_tiles_per_row_v2('NEW_GRID', 1698));
