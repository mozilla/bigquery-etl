
# Firefox Newtab Clients Daily Aggregates

This dataset provides a daily summary of user engagement with the Firefox Newtab page at the granularity of `(submission_date, client_id)`. It aggregates visit-level behaviors and settings into client-level metrics for downstream analysis.

---

## 📌 Overview

- **Dataset Purpose**:
  To track engagement, configuration, and experimentation on the Firefox Newtab page with one row per user per day.

- **Table Grain**:
  One row per `(submission_date, client_id)`.

- **Source Table**:
  [`moz-fx-data-shared-prod.firefox_desktop_derived.newtab_visits_daily_v2`](https://sql.telemetry.mozilla.org/moz-fx-data-shared-prod/firefox_desktop_derived/newtab_visits_daily_v2)

- **Primary Use Cases**:
  - Analyzing trends in Newtab engagement over time
  - Segmenting behavior by organic vs. sponsored interactions
  - Measuring reach and impact of features like widgets, weather, search ads
  - Supporting experiments and configuration impact studies

---

## 🧠 How It Works

1. **Visit-Level Input**
   The source data from `firefox_desktop_derived.newtab_visits_daily_v2` includes one row per `(client_id,
   newtab_visit_id, submission_date)` with detailed interactions and configuration state.

2. **Aggregation Logic**
   - Most stable user settings (e.g., `locale`, `os`, `channel`) are resolved using a UDF `mode_last()` across visits.
   - Event counts (e.g., clicks, impressions, dismissals) are summed per client.
   - Boolean visit-level conditions (e.g., `is_default_ui`) are converted into counts of matching visits per client per day.

3. **Behavioral Segmentation**
   - Separate metrics are maintained for organic vs. sponsored content.
   - Includes interaction categories: content, topsites, widgets, search ads, other.

4. **Experiment Metadata**
   Experiment exposure is passed through via the `experiments` field.

---

## 🧾 Key Output Fields

### 🧍 User Metadata
- `client_id`: Unique client identifier (hashed)
- `app_version`, `os`, `channel`, `locale`, `country`: Client context
- `homepage_category`, `newtab_category`: UI configuration
- `geo_subdivision`: First-level location granularity
- `sample_id`: Integer bucket (0–99) for sampling and cohorting

### 🧪 Experiments
- `experiments`: REPEATED RECORD of experiment keys and values (branch, extra)

### ⚙️ User Settings
- `organic_content_enabled`, `sponsored_content_enabled`
- `organic_topsites_enabled`, `sponsored_topsites_enabled`
- `newtab_search_enabled`, `newtab_weather_enabled`
- `topsite_rows`, `topsite_sponsored_tiles_configured`
- `newtab_blocked_sponsors`: Dismissed advertisers list

### 🧭 Visit & Engagement Counts
| Category     | Visit Counts | Event Counts |
|--------------|--------------|--------------|
| Newtab Opens | `all_visits`, `default_ui_visits` |  |
| Engagement   | `any_engagement_visits`, `nonsearch_engagement_visits` | `any_interaction_count`, `nonsearch_interaction_count` |
| Content      | `*_content_engagement_visits` | `*_content_click_count`, `*_content_impression_count` |
| Topsites     | `*_topsite_engagement_visits` | `*_topsite_click_count`, `*_topsite_impression_count` |
| Widgets      | `widget_engagement_visits` | `widget_interaction_count`, `widget_impression_count` |
| Other        | `others_engagement_visits` | `other_interaction_count`, `other_impression_count` |
| Search Ads   | `search_engagement_visits`, `search_ad_click_visits` | `search_ad_click_count`, `search_ad_impression_count` |
| Feedback     | `*_dismissal_visits`, `*_dismissal_count` | `content_thumbs_up_count`, `content_thumbs_down_count` |

### ⏱️ Duration
- `avg_newtab_visit_duration`: Average visit duration in milliseconds

---

## 🗃️ Schema Reference

A full schema with field names, types, and descriptions is available in `schema.yaml`.

---

## ⏱️ Scheduling & Storage

- **DAG**: Runs as part of `bqetl_newtab`
- **Refresh cadence**: Daily
- **Partitioning**: `submission_date` (*required filter*)
- **Clustering**: Likely by `channel`, `country`, or `newtab_category`
- **Retention**: ~775 days

---

## 🧩 Example Query

```sql
SELECT
  submission_date,
  COUNT(DISTINCT client_id) AS active_clients,
  SUM(any_content_click_count) AS total_content_clicks
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.newtab_daily_aggregates`
WHERE
  submission_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
GROUP BY
  submission_date
ORDER BY
  submission_date DESC;
```
