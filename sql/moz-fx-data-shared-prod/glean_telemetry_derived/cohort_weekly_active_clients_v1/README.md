# glean_telemetry_derived.cohort_weekly_active_clients_v1

## Description

A weekly Glean-based table of clients who were active during each week, used to power downstream weekly cohort metrics. It is built from `glean_telemetry_derived.active_users` via the staging layer `cohort_weekly_active_clients_staging`, applying the inclusion filter `is_dau = TRUE`. This table goes back to start of week for date 768 days ago
and to the last completed week

* **Grain:** `client_id × activity_week` (week start = Sunday, aligned with weekly statistics).
* **Scope:** Desktop and Mobile (from the upstream active_users view).
* **Upstream:** `cohort_weekly_active_clients_staging` → sourced from `glean_telemetry_derived.active_users`.
* **Downstream:** `cohort_weekly_statistics_v1` consumes this table to compute weekly cohort activity.
* **Typical fields:** `client_id`, `activity_week` (DATE, week start Sunday), plus selected app/device attribute
