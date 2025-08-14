# Newtab Visits Daily

A daily job that computes one row per New Tab visit (`newtab_visit_id`) per day for Firefox Newtab Homepage. The
dataset captures whether and how a user interacted with Search, Content Articles and Top Sites on the default New Tab
UI,
along with counts, impressions, window size, and visit duration.

## Overview

- **Purpose:** Measure New Tab engagement on sponsored/organic content at a per-visit granularity for analysis and
  reporting.
- **Grain:** 1 row per (`submission_date`, `client_id`, `newtab_visit_id`).
- **Source:** `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`, events unnested from Component `events`.
- **Metrics filter:** Metrics are scoped to visits opened in the **default New Tab UI**.
- **Incrementality:** Runs daily and processes data for a single partition date via `@submission_date`.

## How it works (high level)

1. **Unnest & filter events** for the target `@submission_date`, selecting categories:
   - `newtab.search` (`issued`), `newtab.search.ad` (`click`, `impression`)
   - `pocket` (`click`, `impression`, `dismiss`, `thumb_voting_interaction`)
   - `topsites` (`click`, `impression`, `dismiss`)
   - `newtab` UI signals (`opened`, `closed`, weather*, wallpaper*, topic_selection*, sections*, inline_selection*)
2. **Visit-level rollups** using aggregate event flags & counts per `newtab_visit_id`.
3. **Derived metrics**, including visit duration (close minus open) and window inner size captured at open.
4. **New fields (Aug 2025):** sampling, legacy profile grouping, geo subdivision, experiment metadata, weather flag,
   search engine IDs, topsite configuration, blocked sponsors, and new dismissal/thumbs metrics across content and topsites.

\* See schema for the full list of weather/wallpaper/sections/topic/inline events included.

## Output schema

See `schema.yaml` for complete field names, types, and descriptions.

Key examples:
- Booleans like `is_search_issued`, `is_content_click`, `is_sponsored_topsite_impression` indicate **any** such event occurred in the visit (default UI only).
- Count fields like `any_content_click_count`, `organic_topsite_impression_count`, `search_ad_impression_count` quantify event frequencies within the visit.
- `newtab_visit_duration` is the timestamp difference between `newtab.opened` and `newtab.closed` (ms).
- Window size at open: `newtab_window_inner_height`, `newtab_window_inner_width`.

## Scheduling & storage

- **DAG:** `bqetl_newtab` (daily)
- **Table type:** Client-level
- **Partitioning:** Time-partitioned on `submission_date` (required partition filter)
- **Clustering:** `channel`, `country`, `newtab_category`
- **Expiration:** 775 days

## Parameters

- `@submission_date` (DATE): processes a single day.
