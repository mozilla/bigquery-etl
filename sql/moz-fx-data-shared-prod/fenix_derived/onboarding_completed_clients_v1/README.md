# fenix_derived.onboarding_completed_clients_v1

## Description

One row per Fenix client per day on which they fired the Glean `onboarding.completed` event. A client who fired it on more than one day has a row for each.

* **Grain:** `client_id`, `completed_date`
* **Key fields:** `client_id`, `completed_date`, `sample_id` (a clustering field, not part of the grain)
* **Source:** `fenix.events_stream`
* **Filters:** `event_category = 'onboarding'`, `event_name = 'completed'`, and a non-null `client_id`
* **Downstream:** the view `fenix.onboarding_completed_clients`, which exposes this table unchanged, read by `fenix.retention_clients` to expose `onboarding_completed_by_day_27`

`client_id` is not unique here, so anything joining this table or its view on `client_id` alone fans out. `retention_clients` collapses it with `MIN(completed_date)` and `GROUP BY client_id` in its `onboarding_completions` CTE; any new consumer has to do the same.

## Floor

The table will be backfilled from 2025-01-01, so a client whose completions all fall before that date will have no row here.

That matters downstream: on `retention_clients` those clients will read `NULL` rather than `FALSE`, indistinguishable from clients who never completed. Anyone segmenting retention on the flag should filter `first_seen_date >= '2025-01-01'` so the population is limited to clients whose whole life falls inside the window.

## Runs

One scheduled run per day, writing that day's partition. Re-running a date rewrites that date and cannot reach any other day's rows; against unchanged source data it produces the same rows, and it is how rows that landed in the source after that day's run get picked up.
