-- newtab.determine_tiles_per_row_v1 figures out the number of tiles per row on the Newtab homepage layout
CREATE OR REPLACE FUNCTION newtab.determine_tiles_per_row_v1(
  layout_type STRING,
  newtab_window_inner_width INTEGER
)
RETURNS INTEGER AS (
  CASE
    WHEN newtab_window_inner_width IS NULL
      THEN NULL
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
  assert.equals(NULL, newtab.determine_tiles_per_row_v1('NEW_GRID', NULL)),
  assert.equals(1, newtab.determine_tiles_per_row_v1('SECTION_GRID', 723)),
  assert.equals(2, newtab.determine_tiles_per_row_v1('SECTION_GRID', 724)),
  assert.equals(3, newtab.determine_tiles_per_row_v1('SECTION_GRID', 1122)),
  assert.equals(3, newtab.determine_tiles_per_row_v1('OLD_GRID', 1698)),
  assert.equals(4, newtab.determine_tiles_per_row_v1('NEW_GRID', 1698)),
  assert.equals(4, newtab.determine_tiles_per_row_v1('SECTION_GRID', 1390)),;
