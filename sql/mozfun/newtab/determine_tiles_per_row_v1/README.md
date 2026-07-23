# UDF: Tiles per row on Firefox Newtab

This UDF (`determine_tiles_per_row_v1`) determines the number of tiles displayed per row on the Firefox Newtab page,
based on the layout type and window width.

## 📥 Input Parameters

| Parameter Name             | Type    | Description                                                                                                                                                                                                                                                                        |
|---------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `layout_type`             | STRING  | The layout style of the Newtab page. Can be one of `NOVA_SECTION`, `NOVA_GRID`, `SECTION_GRID`, `NEW_GRID`, or `OLD_GRID` and is computed using the UDF `determine_grid_layout`.<br/> [README](https://github.com/mozilla/bigquery-etl/blob/main/sql/mozfun/newtab/determine_grid_layout_v1/README.md) for more information about the `determine_grid_layout` UDF |
| `newtab_window_inner_width` | INTEGER | The width (in pixels) of the Firefox browser window. An attribute of the newtab `opened` event.                                                                                                                                                                                    |

## 📤 Output

- Returns an `INTEGER` indicating the number of tiles per row based on the logic described below.
- If an unrecognized layout type is passed, the function returns `NULL`.

## 📐 Logic

### For layout type: `SECTION_GRID`

| `newtab_window_inner_width`        | Tiles per row |
|-----------------------------------|----------------|
| `< 724`                           | 1              |
| `724 ≤ width < 1122`              | 2              |
| `1122 ≤ width < 1390`             | 3              |
| `≥ 1390`                          | 4              |

### For layout type: `NEW_GRID` or `OLD_GRID`

| `newtab_window_inner_width`        | Tiles per row |
|-----------------------------------|----------------|
| `< 724`                           | 1              |
| `724 ≤ width < 1122`              | 2              |
| `1122 ≤ width < 1698` or layout is `OLD_GRID` | 3 |
| `≥ 1698` and layout is `NEW_GRID` | 4              |

### For layout type: `Nova` (Includes Section and Grid variants)

| `newtab_window_inner_width`        | Tiles per row |
|-----------------------------------|----------------|
| `< 1024`                           | 1              |
| `1024≤ width < 1366`              | 2              |
| `1366 ≤ width < 1920`             | 3              |
| `1920 ≤ width < 2650`             | 4              |
| `≥ 2650`                          | 6              |

> Note: The largest bin does have room for 6 tiles, skipping width 5

## ✅ Example Usage

```sql
SELECT `determine_tiles_per_row_v1`('SECTION_GRID', 1200); -- Returns 3

SELECT `determine_tiles_per_row_v1`('NEW_GRID', 1700); -- Returns 4

SELECT `determine_tiles_per_row_v1`('OLD_GRID', 1800); -- Returns 3
