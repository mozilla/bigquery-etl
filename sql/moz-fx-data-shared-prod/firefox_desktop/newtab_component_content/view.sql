CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_component_content`
AS
WITH raw_content_info AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_component_content_v1`
  WHERE
    submission_date >= '2024-01-01'  -- earliest date in the table
),
-- the purpose of the final CTEs is to apply the necessary UDFs to get rownumber
content_and_visit_info AS (
  SELECT
    raw_content_info.*,
    mozfun.newtab.determine_grid_layout_v1(
      section_position IS NOT NULL,
      app_version,
      experiments
    ) AS layout_type
  FROM
    raw_content_info
),
add_tiles_per_row AS (
  SELECT
    content_and_visit_info.*,
    mozfun.newtab.determine_tiles_per_row_v1(
      layout_type,
      newtab_window_inner_width
    ) AS num_tiles_per_row,
    -- get the size of the tiles used in rownumber calc
    CASE
    -- for non-sections, all have a size of 1
      WHEN layout_type != 'SECTION_GRID'
        THEN 1
      WHEN format IN ("spoc", "rectangle", "medium-card")
        THEN 1
    -- it takes two small-card to fill a medium-card spot
      WHEN format = 'small-card'
        THEN 0.5
    -- large cards fill two spots
      WHEN format = 'large-card'
        THEN 2
    -- billboard fills the entire row
      WHEN format = 'billboard'
        THEN mozfun.newtab.determine_tiles_per_row_v1(layout_type, newtab_window_inner_width)
      ELSE NULL
    END AS tile_size
  FROM
    content_and_visit_info
)
SELECT
  add_tiles_per_row.*,
  -- COALESCE with 0 because first element will return NULL due to range filter in partition
  -- Will always be position 0
  COALESCE(
    SAFE_CAST(
      FLOOR(
        SUM(tile_size) OVER (
          -- include layout_type in the partition because some visits
          -- have both grid and section layouts, presumably because they occured
          -- during the transition between the two
          PARTITION BY
            newtab_visit_id,
            layout_type
          ORDER BY
            position
          -- do not include row in sum, because the last element in the row will
          -- sum to a multiple of the row number, and dividing will give the next row
          RANGE BETWEEN
            UNBOUNDED PRECEDING
            AND 1 PRECEDING
        ) / num_tiles_per_row
      ) AS INT
    ),
    0
  ) AS row_number
FROM
  add_tiles_per_row
