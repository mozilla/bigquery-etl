CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_component_content`
AS
WITH raw_content_info AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_component_content_v1`
),
-- the purpose of the final CTEs is to apply the necessary UDFs to get rownumber
content_and_visit_info AS (
  SELECT
    raw_content_info.*,
    mozfun.newtab.determine_grid_layout_v1(
      section_position IS NOT NULL,
      app_version,
      experiments
    ) AS layout_type,
  FROM
    raw_content_info
),
add_tiles_per_row AS (
  SELECT
    content_and_visit_info.*,
    mozfun.newtab.determine_tiles_per_row_v1(
      layout_type,
      newtab_window_inner_width
    ) AS num_tiles_per_row
  FROM
    content_and_visit_info
)
SELECT
  add_tiles_per_row.*,
  SAFE_CAST(
    CASE
      WHEN layout_type = 'SECTION_GRID'
        -- the row number is the same as the section position in sections
        THEN section_position
        -- for grid, divide the postition by the number of tiles per row and take the floor
      ELSE FLOOR(position / num_tiles_per_row)
    END AS INT
  ) AS row_number
FROM
  add_tiles_per_row
