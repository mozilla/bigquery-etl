# glean_telemetry_derived.new_profile_churn_clients_v2

## Description

One row per Firefox Desktop new profile per cohort, with a day flag for each of its first 31 days and churn flags built from them. A cohort is all the clients with the same `first_seen_date` in `firefox_desktop_derived.baseline_clients_first_seen_v1`: the day the server first received a baseline ping from the client.

* **Grain:** `client_id`, `cohort_date`
* **Main fields:** `client_id`, `cohort_date`, `sample_id` (a clustering field, not part of the grain), the day flags `day_0` to `day_30`, the churn flags, and `is_desktop_argument_profile_automation`
* **Sources:** `firefox_desktop.baseline_clients_last_seen` for activity, `firefox_desktop_derived.baseline_clients_first_seen_v1` for cohort membership and attributes, and `firefox_desktop_derived.desktop_dau_distribution_id_history_v1` for `distribution_id` where Glean lacks it
* **Companion table:** `glean_telemetry_derived.recent_new_profile_churn_clients_v2` holds the same columns for cohorts too young to be complete. Its `cohort_cutoff` starts the day after the newest cohort here, so the two tables never hold the same cohort.
* **Downstream:** the `glean_telemetry.new_profile_churn_clients` view will union this table with `recent_new_profile_churn_clients_v2` once it switches to v2 (DENG-11734); until then it reads the v1 tables. The view and this table require a filter on `cohort_date`.

## Flags

A day flag `day_k` is `TRUE` when the client was active on day `k` after its first-seen day, `FALSE` when it was not, and `NULL` while the cohort has not yet reached day `k`. Active means the client's baseline pings that day showed engagement, counted on the day the pings arrived, so a late ping counts on a later day.

Each churn flag is `NULL` until the cohort has the days of data it needs, then `TRUE` or `FALSE`:

| Flag | `TRUE` when | `NULL` while `days_data_available` is below |
| --- | --- | --- |
| `churn_risk_after_5_days` | no activity on days 1 to 4 | 5 |
| `churn_risk_after_7_days` | no activity on days 2 to 6 | 7 |
| `immediately_churned` | no activity on days 0 to 27 | 28 |
| `churned_after_1_day` | active on day 0, none on days 1 to 28 | 29 |
| `churned_after_2_days` | active on day 1, none on days 2 to 29 | 30 |

A churn rate divides by the flags that have a value, `COUNTIF(flag) / COUNT(flag)`, so cohorts too young for a flag drop out. Dividing by `COUNT(*)` counts them as not churned.

`is_desktop_argument_profile_automation` is `TRUE` for the automated segment tracked in DENG-11590, per `udf.is_desktop_argument_profile_automation`. Those profiles stay in this table for ad hoc analysis; exclude them from counts with `WHERE NOT is_desktop_argument_profile_automation`, as the aggregate (DENG-11664) will by default.

## Runs

One scheduled run per day, in `bqetl_new_profile_churn`, writing the cohort 30 days before the run date into that cohort's partition. Re-running a date rewrites that cohort only.

## Floor

Partitions expire after 775 days, matching `firefox_desktop_derived.baseline_clients_last_seen_v1`. A backfill will fill every cohort from the oldest available partition of that table onward; until it completes, the table holds only the cohorts written since deploy.
