# glean_telemetry_derived.new_profile_churn_clients_v2

## Description

One row per Firefox Desktop new profile per cohort, with a day flag for each of its first 31 days and churn flags built from them. A cohort is all the clients with the same `first_seen_date` in `firefox_desktop_derived.baseline_clients_first_seen_v1`: the day the server first received a baseline ping from the client.

* **Grain:** `client_id`, `cohort_date`
* **Main fields:** `client_id`, `cohort_date`, `sample_id` (a clustering field, not part of the grain), the day flags `day_0` to `day_30`, the churn flags, `is_desktop_argument_profile_automation` and `is_default_browser`
* **Sources:** `firefox_desktop.baseline_clients_last_seen` for activity, `firefox_desktop_derived.baseline_clients_first_seen_v1` for cohort membership and attributes, `firefox_desktop_derived.desktop_dau_distribution_id_history_v1` for `distribution_id` where Glean lacks it, and `firefox_desktop_derived.baseline_clients_daily_v1` for `is_default_browser`
* **Companion table:** `glean_telemetry_derived.recent_new_profile_churn_clients_v2` holds the same columns for cohorts too young to be complete. Its `cohort_cutoff` starts the day after the newest cohort here, so the two tables hold the same cohort only briefly during a daily run. The `glean_telemetry.new_profile_churn_clients` view reads `new_profile_churn_clients_v2` only for cohorts before the earliest cohort in `recent_new_profile_churn_clients_v2`, so the view takes any shared cohort from `recent_new_profile_churn_clients_v2`.
* **Downstream:** the `glean_telemetry.new_profile_churn_clients` view unions this table with `recent_new_profile_churn_clients_v2`. The view and this table require a filter on `cohort_date`.

## Flags

`TRUE` and `FALSE` mean the same in this table and in `recent_new_profile_churn_clients_v2`. Every cohort in this table has `days_data_available` 31, so every day flag and churn flag here is `TRUE` or `FALSE`. In `recent_new_profile_churn_clients_v2`, a day flag or churn flag is `NULL` until its cohort has the days of data the flag needs.

A day flag `day_k` is `TRUE` when the client was active on day `k` after its first-seen day and `FALSE` when it was not. In `recent_new_profile_churn_clients_v2`, it is `NULL` while the cohort has not yet reached day `k`. Active is the desktop daily-active-user definition: the client's baseline pings that day recorded at least one URI loaded and some active time (`browser_engagement_uri_count` and `browser_engagement_active_ticks` both above 0). The day is the day the pings arrived, so a late ping counts on a later day.

Each churn flag:

| Flag | `TRUE` when | `NULL` in `recent_new_profile_churn_clients_v2` while `days_data_available` is below |
| --- | --- | --- |
| `churn_risk_after_5_days` | no activity on days 1 to 4 | 5 |
| `churn_risk_after_7_days` | no activity on days 2 to 6 | 7 |
| `immediately_churned` | no activity on days 0 to 27 | 28 |
| `churned_after_1_day` | active on day 0, none on days 1 to 28 | 29 |
| `churned_after_2_days` | active on day 1, none on days 2 to 29 | 30 |

In `recent_new_profile_churn_clients_v2` and the `glean_telemetry.new_profile_churn_clients` view, a cohort too young for a churn flag has no value for it. Counts and rates of a churn flag cover only the clients whose flag has a value, so those cohorts drop out; counting them as not churned understates churn.

`is_desktop_argument_profile_automation` is `TRUE` for the automated segment tracked in DENG-11590, per `udf.is_desktop_argument_profile_automation`. Those profiles stay in this table for ad hoc analysis; exclude them from counts with `WHERE NOT is_desktop_argument_profile_automation`, as the aggregate (DENG-11664) will by default.

`is_default_browser` is whether Firefox was the default browser on the client's first-seen day, the most common value across that day's baseline pings. It is `NULL` when none of that day's pings reported it, or when `baseline_clients_daily_v1` has no row for the client that day, as for a client it drops for sending over 150,000 pings.

## Runs

One scheduled run per day, in `bqetl_new_profile_churn`, writing the cohort 30 days before the run date into that cohort's partition. Re-running a date rewrites that cohort only.

## History

Partitions expire after 775 days, matching `firefox_desktop_derived.baseline_clients_last_seen_v1`. A backfill filled every cohort from the oldest partition of that table available at the time onward.
