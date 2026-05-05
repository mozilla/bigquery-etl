# glean_telemetry_derived.rolling_cohorts_v1

## Description

A Glean-based “rolling cohorts” table with **one row per client and row_source per cohort date** (each date represents a new cohort of clients who sent their first ping that day). This table can be left-joined to activity tables to compute cohort activity metrics.

* **Grain:** `client_id × row_source × cohort_date`
* **Key fields:** `client_id`, `cohort_date` (first seen date), `activity_segment`, plus additional client/app attributes.
* **Filters:** None. No data excluded. BrowswerStack and MozillaOnline data can be found in this table
* **Downstream:** `cohort_daily_statistics_v1`, `cohort_daily_churn_v1`, and `cohort_weekly_statistics_v1`, `cohort_weekly_cfs_staging_v1` are built from `rolling_cohorts_v1`.
