# New Tab Visits Daily v2

## Overview

The `firefox_desktop_derived.newtab_visits_daily_v2` table provides comprehensive daily aggregations of user interactions with Firefox Desktop's New Tab page. This table captures detailed engagement metrics including content interactions (Pocket), topsite clicks, search behavior, widget usage, and wallpaper customization events.

Each row represents a single new tab visit session, aggregated by submission date, client, and visit ID, providing both high-level engagement flags and granular interaction counts.

## Data Source

**Primary Source:** `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`

The query processes telemetry events from the Firefox Desktop new tab ping, filtering for specific interaction events across multiple categories:
- Search events (`newtab.search`, `newtab.search.ad`)
- Pocket content events (`pocket`)
- Topsite events (`topsites`)
- New tab lifecycle events (`newtab`)
- Widget, wallpaper, and customization events

## Key Features

### Visit-Level Metrics
- Unique visit identification via `newtab_visit_id`
- Visit duration tracking (opened to closed)
- Browser window dimensions
- Default UI detection

### Content Interaction Tracking
- **Pocket Content**: Clicks, impressions, dismissals, and thumbs up/down voting
- **Topsites**: Clicks, impressions, dismissals, pin/unpin actions
- **Differentiation**: Organic vs. sponsored content metrics
- **Position-Level Detail**: New `content_position_components` and `topsite_tile_components` arrays provide granular engagement data per position

### Search & Advertising
- Search issuance tracking
- Search ad clicks and impressions
- Default search engine configuration

### Widgets & Customization
- Weather widget interactions and impressions
- List and timer widget usage
- Wallpaper selection and customization
- Topic selection and section following

### Configuration & Experiments
- Feature enablement flags (sponsored content, weather, search)
- Active experiment tracking
- Blocked sponsors list
- Topsite configuration (rows, sponsored tiles)

## Update Frequency

**Daily** - Partitioned by `submission_date` using the `@submission_date` parameter

## Key Columns

| Column | Description | Use Case |
|--------|-------------|----------|
| `submission_date` | Date of data submission | Partitioning, time-series analysis |
| `client_id` | Unique client identifier | User-level aggregation |
| `newtab_visit_id` | Unique visit session ID | Visit-level analysis |
| `is_default_ui` | Default UI indicator | Filtering for standard experience |
| `*_click_count` | Various click metrics | Engagement measurement |
| `*_impression_count` | Various impression metrics | Exposure measurement |
| `sponsored_*` vs `organic_*` | Content type metrics | Revenue and organic engagement comparison |
| `topsite_tile_components` | Per-tile metrics array | Detailed topsite position analysis |
| `content_position_components` | Per-position content metrics | Detailed content position analysis |

## Important Notes

- **Default UI Filtering**: Most interaction metrics are only counted when `is_default_ui = TRUE`
- **Aggregation Level**: One row per (submission_date, client_id, newtab_visit_id)
- **NULL Handling**: `ANY_VALUE()` is used for fields expected to be consistent within a visit
- **New in v2**: Added position-level component arrays (`topsite_tile_components`, `content_position_components`) and support for pin/unpin events

## Example Use Cases

### 1. Sponsored Content Performance Analysis
```sql
SELECT
  submission_date,
  COUNT(DISTINCT client_id) AS unique_clients,
  SUM(sponsored_content_click_count) AS total_sponsored_clicks,
  SUM(sponsored_content_impression_count) AS total_sponsored_impressions,
  SAFE_DIVIDE(
    SUM(sponsored_content_click_count),
    SUM(sponsored_content_impression_count)
  ) AS sponsored_ctr
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
WHERE
  submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND is_default_ui = TRUE
GROUP BY
  submission_date
ORDER BY
  submission_date DESC;
```

### 2. Widget Adoption and Engagement
```sql
SELECT
  country,
  COUNTIF(newtab_weather_enabled) AS weather_enabled_visits,
  COUNTIF(widget_interaction_count > 0) AS visits_with_widget_interaction,
  SUM(widget_interaction_count) AS total_widget_interactions,
  AVG(widget_interaction_count) AS avg_interactions_per_visit
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`
WHERE
  submission_date = CURRENT_DATE() - 1
  AND is_default_ui = TRUE
GROUP BY
  country
ORDER BY
  weather_enabled_visits DESC
LIMIT 10;
```

### 3. Topsite Position-Level Performance
```sql
SELECT
  tile.position,
  tile.is_sponsored,
  COUNT(*) AS tile_occurrences,
  SUM(tile.impression_count) AS total_impressions,
  SUM(tile.click_count) AS total_clicks,
  SUM(tile.pin_count) AS total_pins,
  SAFE_DIVIDE(SUM(tile.click_count), SUM(tile.impression_count)) AS ctr
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`,
  UNNEST(topsite_tile_components) AS tile
WHERE
  submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND is_default_ui = TRUE
GROUP BY
  tile.position,
  tile.is_sponsored
ORDER BY
  tile.position,
  tile.is_sponsored;
```

### 4. Experiment Impact on Engagement
```sql
SELECT
  exp.value AS experiment_branch,
  COUNT(DISTINCT client_id) AS unique_clients,
  AVG(any_interaction_count) AS avg_interactions,
  AVG(newtab_visit_duration) / 1000 AS avg_visit_duration_seconds,
  COUNTIF(is_search_issued) / COUNT(*) AS search_rate
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`,
  UNNEST(experiments) AS exp
WHERE
  submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND exp.key = 'your-experiment-id'
  AND is_default_ui = TRUE
GROUP BY
  experiment_branch;
```

## Schema Version

**Version:** v2

**Major Changes from v1:**
- Added `topsite_tile_components` array for detailed per-tile metrics
- Added `content_position_components` array for detailed per-position content metrics
- Added support for pin/unpin topsite events
- Enhanced position-level tracking for both topsites and Pocket content

## Related Tables

- `firefox_desktop_stable.newtab_v1` - Source table for raw new tab events
- `firefox_desktop.newtab_visits` - User-facing view of this derived table

## Maintainer Notes

- The query uses CTEs to separate concerns: event filtering, core metrics, and component-level aggregations
- Default UI filtering ensures metrics reflect the standard user experience
- The `@submission_date` parameter enables daily incremental processing
- Position-level components enable advanced spatial analysis of new tab engagement

## Scheduling & storage

- **DAG:** `bqetl_newtab` (daily)
- **Table type:** Client-level
- **Partitioning:** Time-partitioned on `submission_date` (required partition filter)
- **Clustering:** `channel`, `country`, `newtab_category`
- **Expiration:** 775 days
