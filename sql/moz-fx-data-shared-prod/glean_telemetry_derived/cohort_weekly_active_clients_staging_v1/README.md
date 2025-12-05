# cohort_weekly_active_clients_staging

## Description

A staging table of Glean active users used to build weekly cohort metrics. It pulls from the view `glean_telemetry_derived.active_users`, which contains data for **both Desktop and Mobile**, and applies a single inclusion filter where `is_dau` is **TRUE**. This staging layer exists to reduce processing volume for downstream weekly aggregation.

* **Upstream:** `glean_telemetry_derived.active_users`
* **Filter:** `is_dau = TRUE` (no other filters)
* **Scope:** Desktop and Mobile
* **Downstream:** `cohort_weekly_statistics_v1` consumes this table to compute weekly cohort activity
* **Typical fields:** `client_id`, `submission_date` (daily activity); weekly bucketing is performed in downstream logic.
