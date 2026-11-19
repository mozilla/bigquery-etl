# Widgets Client Daily v1

## Overview

The `firefox_desktop_derived.widgets_client_daily_v1` table provides per-client, per-widget aggregations of Firefox Desktop newtab widget telemetry. It rolls up `firefox_desktop_derived.widgets_visit_daily_v1` across all visits a client had in a day, producing one row per `(client_id, widget_name, widget_size)` per `submission_date`.

Each row summarizes how many distinct visits a client had where a given widget was visible or interacted with, total event counts across those visits, and a per-`user_action` breakdown of `widgets_user_event` events aggregated across visits.

## Data Source

**Primary source:** `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_visit_daily_v1`

This table is a pure rollup — no new event-level processing happens here. All event semantics are inherited from `widgets_visit_daily_v1`. See that table's [README](../widgets_visit_daily_v1/README.md) for source-event details.

## Grain

One row per `(submission_date, client_id, widget_name, widget_size)`.

A row is emitted only when the client had at least one visit in which the widget+size combination produced an event. Widget+size combinations that the client never saw or interacted with do not contribute rows.

## Aggregation Strategy

Aggregation patterns mirror `newtab_clients_daily_v2`:

- **`udf.mode_last`** for categorical visit-level dimensions that may shift between visits within a day (`app_version`, `os`, `os_version`, `windows_build_number`, `channel`, `locale`, `country`, `geo_subdivision`, `homepage_category`, `newtab_category`, all `*_enabled` flags, search engine settings, topsite config, `newtab_content_surface_id`). Picks the most-common value with last-seen as the tie-breaker.
- **`ANY_VALUE`** for fields stable per client (`sample_id`, `legacy_telemetry_client_id`, `profile_group_id`, `experiments`, `newtab_blocked_sponsors`).
- **`COUNT(DISTINCT IF(... , newtab_visit_id, NULL))`** for visit-level engagement metrics, counting distinct visits where the predicate held.
- **`SUM`** for event counts rolled up across visits.
- **`UNNEST` + re-aggregate** for `user_action_counts`: per-visit arrays are unnested, summed per `user_action`, then re-aggregated into a single array.

## Key Columns

### Identifiers
| Column | Description |
|---|---|
| `submission_date` | Day the underlying events were received |
| `client_id` | Firefox Desktop client installation ID |
| `widget_name` | Widget the row aggregates (e.g. `lists`, `focus_timer`, `weather`) |
| `widget_size` | Widget size for the row (e.g. `mini`, `small`, `medium`) |

### Visit Counts
| Column | Description |
|---|---|
| `all_visits` | Distinct newtab visits the client had this day in which this widget+size had any qualifying event |
| `default_ui_visits` | Subset of `all_visits` where `is_default_ui = TRUE` |
| `widget_impression_visits` | Visits with at least one `widgets_impression` event for this widget+size |
| `widget_user_event_visits` | Visits with at least one `widgets_user_event` event for this widget+size |
| `widget_enabled_visits` | Visits with at least one `widgets_enabled (enabled=TRUE)` event for this widget+size |
| `widget_disabled_visits` | Visits with at least one `widgets_enabled (enabled=FALSE)` event for this widget+size |

### Event Counts (summed across visits)
| Column | Description |
|---|---|
| `impression_count` | Total `widgets_impression` events for this widget+size across all visits this day |
| `user_event_count` | Total `widgets_user_event` events for this widget+size across all visits this day |
| `change_size_or_learn_more_count` | Subset of `user_event_count` whose `user_action` is `change_size` or `learn_more` |
| `enabled_count` | Total `widgets_enabled` events with `enabled = TRUE` |
| `disabled_count` | Total `widgets_enabled` events with `enabled = FALSE` |
| `user_action_counts` | Array of `(user_action, count)` structs summed across all visits this day |

### Visit-level Context
| Column | Description |
|---|---|
| `experiments` | Array of experiment enrollments associated with the client |
| `sample_id`, `legacy_telemetry_client_id`, `profile_group_id` | Stable per-client identifiers carried through unchanged |

Plus mode-last'd Firefox dimensions (`channel`, `country`, `locale`, `os`, `os_version`, `windows_build_number`, `app_version`, `geo_subdivision`, `homepage_category`, `newtab_category`, etc.).

## Important Notes

- **No `newtab_visit_id` in this table.** If you need per-visit detail use `widgets_visit_daily_v1` directly.
- **`all_visits` is widget-scoped.** It only counts visits where the widget+size of this row had at least one event — not all newtab visits the client had. For total newtab visits per client, query `newtab_clients_daily_v2`.
- **`user_action_counts` is summed across the client's visits.** A user_action that occurred in 3 different visits shows up as a single struct with the combined count.
- **Dimensions are "mode within the day".** If a client's `country` or `channel` legitimately changed between visits, the row reflects the most-common value, not all values.
- Because widget_size is used as a dimension, impressions and interactions might be spread across sizes in unexpected way.  For instance, if there is only one visit and if a medium sized widget is changed to large and then clicked, the medium widget row will have 1 impression and 0 interactions, while the large widget row will have 0 impressions and 1 interaction.

## Example Use Cases

### Daily active clients per widget
```sql
SELECT
  submission_date,
  widget_name,
  widget_size,
  COUNT(DISTINCT client_id) AS active_clients,
  SUM(impression_count) AS impressions,
  SUM(user_event_count) AS user_events,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_client_daily`
WHERE
  submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY
  submission_date, widget_name, widget_size
ORDER BY
  submission_date, widget_name, widget_size;
```

### Engagement rate (% of clients with any user interaction)
```sql
SELECT
  widget_name,
  COUNT(DISTINCT client_id) AS clients_seeing_widget,
  COUNT(DISTINCT IF(user_event_count > 0, client_id, NULL)) AS clients_interacting,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(user_event_count > 0, client_id, NULL)),
    COUNT(DISTINCT client_id)
  ) AS engagement_rate,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_client_daily`
WHERE
  submission_date = CURRENT_DATE() - 1
GROUP BY
  widget_name;
```

### Most common user actions per widget
```sql
SELECT
  widget_name,
  ua.user_action,
  SUM(ua.count) AS interactions,
  COUNT(DISTINCT client_id) AS interacting_clients,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_client_daily`,
  UNNEST(user_action_counts) AS ua
WHERE
  submission_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE() - 1
GROUP BY
  widget_name, ua.user_action
ORDER BY
  widget_name, interactions DESC;
```

### Net enables vs disables
```sql
SELECT
  widget_name,
  SUM(enabled_count) AS total_enables,
  SUM(disabled_count) AS total_disables,
  SUM(enabled_count) - SUM(disabled_count) AS net_enable_delta,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_client_daily`
WHERE
  submission_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE() - 1
GROUP BY
  widget_name;
```

## Related Tables

- `firefox_desktop_derived.widgets_visit_daily_v1` — per-visit source for this table
- `firefox_desktop.widgets_client_daily` — user-facing view of this table; adds `app_name` and a derived `windows_version` column
- `firefox_desktop_derived.newtab_clients_daily_v2` — broader client-level newtab aggregation (no per-widget grain)

## Scheduling & Storage

- **DAG:** `bqetl_newtab` (daily)
- **Table type:** client-level
- **Partitioning:** time-partitioned on `submission_date` (required partition filter)
- **Clustering:** `sample_id`, `client_id`, `widget_name`, `country`
