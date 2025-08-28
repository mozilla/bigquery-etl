CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.newtab_visits_daily`
AS
WITH src AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
),
enriched AS (
  SELECT
    src.*,
    -- Precompute once for reuse
    mozfun.newtab.determine_grid_layout_v1(is_section, app_version, experiments) AS layout_type,
  FROM
    src
)
SELECT
  'Firefox Desktop' AS app_name,
  layout_type,
  mozfun.newtab.determine_tiles_per_row_v1(layout_type, newtab_window_inner_width) AS tiles_per_row,
  enriched.* EXCEPT (layout_type)
FROM
  enriched
