# fenix_derived.onboarding_completed_clients_v1

## Description

One row per Fenix client per day on which an `onboarding.completed` event from them arrived, dated by submission date rather than by when the event fired. A client with completions arriving on more than one day has a row for each.

* **Grain:** `client_id`, `completed_date`
* **Key fields:** `client_id`, `completed_date`, `sample_id` (a clustering field, not part of the grain), `app_version`
* **Source:** `fenix.events_stream`
* **Filters:** `event_category = 'onboarding'`, `event_name = 'completed'`, and a non-null `client_id`
* **Downstream:** the `fenix.onboarding_completed_clients` view exposes this table unchanged. `fenix.retention_clients` reads that view to add two columns, `is_onboarded` and `app_version_at_onboarding_completion`. Both consider only completions on or before the row's `submission_date`: the client's day 27 on `new_profile` rows, and 27 days after that row's `metric_date` on later rows.

`client_id` is not unique here, so anything joining this table or its view on `client_id` alone fans out. Collapse on `client_id` first, and take `app_version` from the same row you take the date from. For example, `retention_clients` does this in its `onboarding_completions` CTE.

`app_version` is the version the client was running at the completion, taken from the earliest completion event among those reported on that date, and `NULL` where the event carried none. Where two events share an event timestamp the version comes from whichever ping arrived first, and where those match too, the lower version string. It is the raw version string, so it sorts lexicographically — `'100.0'` orders below `'9.0'`. It is not the `app_version` on `retention_clients`, which is as of that row's metric date; and since `completed_date` is the reported day, a ping arriving after an upgrade carries the later day's version.

## Floor

Coverage will begin 2025-01-01 when the table is backfilled; until then it holds only the days since it was deployed. Partitions do not expire, so the floor will stay at 2025-01-01. A client whose completions all fall before 2025-01-01 will have no row here.

That sets what `is_onboarded` on `retention_clients` can say. It will read `FALSE` only for clients first seen on or after 2025-01-01 with no completion on record by the row's `submission_date`, and `NULL` for clients first seen before that date or with no `first_seen_date` who have no completion on record by then, since their completions may predate the table. A completion whose ping arrived after the row's `submission_date` counts as not yet on record, so a late ping reads as `FALSE`, or `NULL` for a client first seen before 2025-01-01. A completion rate should filter on `first_seen_date >= '2025-01-01'`: clients first seen earlier read `TRUE` when they completed and `NULL` when they did not, so including them overstates the rate. Until the backfill completes, clients first seen on or after 2025-01-01 whose completions are not yet loaded will also read `FALSE`.

## Runs

One scheduled run per day, writing that day's partition. Re-running a date rewrites that date and cannot reach any other day's rows; against unchanged source data it produces the same rows, and it is how rows that landed in the source after that day's run get picked up.
