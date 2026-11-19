# Widgets Visit Daily v1

## Overview

The `firefox_desktop_derived.widgets_visit_daily_v1` table provides per-visit, per-widget aggregations of Firefox Desktop newtab widget telemetry. It unpacks the `widgets_impression`, `widgets_user_event`, and `widgets_enabled` events from the `newtab` event category and rolls them up to one row per newtab visit per widget configuration.

Each row represents the combination of a `(newtab_visit_id, widget_name, widget_size)` for a given submission date, capturing how many impressions and user actions the widget received, whether the widget was enabled or disabled during the visit, and a per-`user_action` breakdown of `widgets_user_event` events.

## Data Source

**Primary source:** `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`

Events processed (all under the `newtab` event category):

- `widgets_impression` — emitted when a widget becomes viewable on the user's screen
- `widgets_user_event` — emitted when the user interacts with a widget (e.g. `list_copy`, `timer_set`, `change_size`, `learn_more`)
- `widgets_enabled` — emitted when the user enables or disables a widget; the `enabled` extra distinguishes the two states
- `newtab.opened` — also included so that the visit-level `is_default_ui` flag can be derived via `mozfun.newtab.is_default_ui_v1`

See the Glean Dictionary entries for [`newtab.widgets_impression`](https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/newtab_widgets_impression), [`newtab.widgets_user_event`](https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/newtab_widgets_user_event), and [`newtab.widgets_enabled`](https://dictionary.telemetry.mozilla.org/apps/firefox_desktop/metrics/newtab_widgets_enabled) for the canonical event descriptions and extra-key definitions.

## Grain

One row per `(submission_date, client_id, newtab_visit_id, widget_name, widget_size)`.

A row is emitted only when a widget had at least one `widgets_impression`, `widgets_user_event`, or `widgets_enabled` event in the visit; visits with no widget activity for a given widget+size do not contribute rows.

## Key Columns

### Identifiers
| Column | Description |
|---|---|
| `submission_date` | Day the event was received in the newtab ping |
| `client_id` | Firefox Desktop client installation ID |
| `newtab_visit_id` | Unique identifier for the newtab visit |
| `widget_name` | Widget that emitted the event (e.g. `lists`, `focus_timer`, `weather`) |
| `widget_size` | Size of widget (e.g. `mini`, `small`, `medium`) |

### Event Counts
| Column | Description |
|---|---|
| `impression_count` | Number of `widgets_impression` events for this widget within the visit |
| `user_event_count` | Number of `widgets_user_event` events for this widget within the visit |
| `change_size_or_learn_more_count` | Number of `widgets_user_event` events whose `user_action` is `change_size` or `learn_more` |
| `enabled_count` | Number of `widgets_enabled` events where `enabled = TRUE` |
| `disabled_count` | Number of `widgets_enabled` events where `enabled = FALSE` |
| `user_action_counts` | Array of `(user_action, count)` structs giving per-action counts for `widgets_user_event` events |

### Visit-level Context
| Column | Description |
|---|---|
| `is_default_ui` | `TRUE` if the newtab open was in the default UI (per `mozfun.newtab.is_default_ui_v1`) |
| `submission_timestamp` | Earliest ping submission_timestamp contributing to this visit |
| `experiments` | Array of experiment enrollment structs associated with the visit |

Plus standard Firefox dimensions (`channel`, `country`, `locale`, `os`, `os_version`, `windows_build_number`, `app_version`, `sample_id`, `legacy_telemetry_client_id`, `profile_group_id`, etc.) propagated from the source ping via `ANY_VALUE`.

## Important Notes

- **`is_default_ui` is informational, not a filter.** All visits with widget events are emitted regardless of UI mode; downstream consumers can filter on `is_default_ui` if needed.
- **`change_size_or_learn_more_count`** counts a specific subset of `widgets_user_event` events surfaced as its own column because those interactions tend to be analyzed separately from primary engagement actions.
- **Per-user_action detail lives in `user_action_counts`.** For analyses that care about specific `user_action` values not surfaced as their own column, `UNNEST(user_action_counts)`.
- Because widget_size is used as a dimension, impressions and interactions might be spread across sizes in unexpected way.  For instance, if a medium sized widget is changed to large and then clicked, the medium widget row will have 1 impression and 0 interactions, while the large widget row will have 0 impressions and 1 interaction.

## Example Use Cases

### Daily impressions per widget
```sql
SELECT
  submission_date,
  widget_name,
  widget_size,
  COUNT(DISTINCT client_id) AS clients,
  COUNT(DISTINCT newtab_visit_id) AS visits,
  SUM(impression_count) AS impressions,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_visit_daily`
WHERE
  submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND is_default_ui
GROUP BY
  submission_date, widget_name, widget_size
ORDER BY
  submission_date, widget_name, widget_size;
```

### Per-`user_action` engagement
```sql
SELECT
  widget_name,
  ua.user_action,
  SUM(ua.count) AS interactions,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_visit_daily`,
  UNNEST(user_action_counts) AS ua
WHERE
  submission_date = CURRENT_DATE() - 1
  AND is_default_ui
GROUP BY
  widget_name, ua.user_action
ORDER BY
  interactions DESC;
```

### Enable / disable rate per widget
```sql
SELECT
  widget_name,
  SUM(enabled_count) AS enables,
  SUM(disabled_count) AS disables,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.widgets_visit_daily`
WHERE
  submission_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE() - 1
GROUP BY
  widget_name;
```

## Related Tables

- `firefox_desktop_stable.newtab_v1` — source ping table
- `firefox_desktop.widgets_visit_daily` — user-facing view of this table; adds `app_name` and a derived `windows_version` column
- `firefox_desktop_derived.widgets_client_daily_v1` — client-level rollup of this table
- `firefox_desktop_derived.newtab_visits_daily_v2` — broader newtab-visit aggregation that includes widget-related fields alongside topsites, content, search, etc.

## Scheduling & Storage

- **DAG:** `bqetl_newtab` (daily)
- **Table type:** client-level
- **Partitioning:** time-partitioned on `submission_date` (required partition filter)
- **Clustering:** `channel`, `country`, `widget_name`, `sample_id`
