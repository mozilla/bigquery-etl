# Newtab Visits Daily

A view for the `newtab_visits_daily_v2` table that exposes per-visit, per-day New Tab metrics for Firefox Desktop. The
dataset’s grain is one row per
`newtab_visit_id` for each `submission_date`, suitable for adhoc analysis of daily visit metrics.

## View details
- **View:** `moz-fx-data-shared-prod.firefox_desktop.newtab_visits_daily`
- **Source table:** `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
- **Pass-through:** Includes all columns from the source via `*`.
- **Additional static fields introduced:**
  - **App label:** Adds a constant column `app_name = 'Firefox Desktop'`.

## Derived fields in the view
- **is_any_interaction:** True if *any* of search, content, topsite, widget, wallpaper, or other interactions occurred.
- **is_nonsearch_interaction:** True if any non-search interaction occurred (content, topsite, widget, wallpaper, other).
- **is_organic_content_interaction:** Content interaction that is **not** sponsored.
- **is_organic_topsite_interaction:** Topsite interaction that is **not** sponsored.
- **layout_type:** Computed using udf `mozfun.newtab.determine_grid_layout_v1(is_section, app_version, experiments)`. [README](https://github.com/mozilla/bigquery-etl/blob/main/sql/mozfun/newtab/determine_grid_layout_v1/README.md) for more information about the `determine_grid_layout` UDF.
- **tiles_per_row:** Computed using udf `mozfun.newtab.determine_tiles_per_row_v1(layout_type,
newtab_window_inner_width)`[README](https://github.com/mozilla/bigquery-etl/blob/main/sql/mozfun/newtab/determine_tiles_per_row_v1/README.md) for more information about the `determine_tiles_per_row` UDF.
