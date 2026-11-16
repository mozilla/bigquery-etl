# glean_telemetry_derived.new_profile_churn_aggregates_v1

## Description

New-profile churn and early churn-risk counts for Firefox Desktop, one row per cohort and combination of audience columns. A cohort is all the clients with the same `first_seen_date`.

* **Grain:** `cohort_date` and the 15 audience columns: `country`, `locale`, `distribution_id`, `normalized_channel`, `normalized_os`, `normalized_os_version`, `os_version_major`, `os_version_minor`, `app_display_version`, `app_name`, `first_seen_attribution_campaign`, `first_seen_attribution_content`, `first_seen_attribution_medium`, `first_seen_attribution_source` and `is_default_browser`
* **Main fields:** `cohort_clients_size`, the precursor counts and the churn counts
* **Source:** the `glean_telemetry.new_profile_churn_clients` view, which unions `glean_telemetry_derived.new_profile_churn_clients_v2` and `recent_new_profile_churn_clients_v2`. The counts cover only clients with `is_desktop_argument_profile_automation` `FALSE`, leaving out the [DENG-11590](https://mozilla-hub.atlassian.net/browse/DENG-11590) bot profiles.

## Metrics

Each count covers the clients in its row's cohort and audience group. Churn is 28 consecutive days without activity.

| Metric | Counts clients who | Days of data needed |
| --- | --- | --- |
| `cohort_clients_size` | are in the cohort and audience group | 1 |
| `clients_inactive_first_day` | were inactive on day 0 | 1 |
| `clients_churn_risk_after_5_days` | were inactive on days 1 to 4 | 5 |
| `clients_churn_risk_after_7_days` | were inactive on days 2 to 6 | 7 |
| `clients_churned_immediately` | were inactive on days 0 to 27 | 28 |
| `clients_churned_after_1_day` | were active on day 0, then inactive on days 1 to 28 | 29 |
| `clients_churned_after_2_days` | were active on day 1, then inactive on days 2 to 29 | 30 |
| `cumulative_clients_churned_after_1_day` | churned immediately or after 1 day | 29 |
| `cumulative_clients_churned_after_2_days` | churned immediately, after 1 day or after 2 days | 30 |

The three precursor counts, `clients_inactive_first_day`, `clients_churn_risk_after_5_days` and `clients_churn_risk_after_7_days`, need only days 0 to 6 of activity, so they are available weeks before the churn counts. A client is in at most one churn group, so the cumulative counts add the churn counts without counting any client twice.

## Reading the counts

* Each churn and churn-risk count is `NULL` until its cohort has the days of data the count needs, then a number. Once a count has a value, dividing it by `cohort_clients_size` gives the rate for that group.
* `days_data_available` comes from the client tables, so a complete cohort reads 31, since the client tables track days 0 to 30.
* `is_default_browser` is `NULL` in every row for cohorts before 2025-01-30, the first date `firefox_desktop_derived.baseline_clients_daily_v1` has it.
* The high-cardinality audience columns leave many rows holding only a few clients; sum across rows for larger groups.
* DS measures how well the precursors predict churn from the client-level view, where each client's precursor and churn flags share a row; this table holds the separate counts only.

## Runs

One scheduled run per day, in `bqetl_new_profile_churn`, rebuilding the whole table from the last 775 days of cohorts, the client tables' retention.
