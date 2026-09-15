# fenix_derived.onboarding_completed_clients_v1

## Description

One row per Fenix client per day on which they fired the Glean `onboarding.completed` event. A client who fired it on more than one day has a row for each.

* **Grain:** `client_id`, `completed_date`
* **Key fields:** `client_id`, `completed_date`, `app_version`, `sample_id` (a clustering field, not part of the grain)
* **Source:** `fenix.events_stream`
* **Filters:** `event_category = 'onboarding'`, `event_name = 'completed'`, and a non-null `client_id`
* **Downstream:** the view `fenix.onboarding_completed_clients`, which exposes this table unchanged, read by `fenix.retention_clients` to expose `onboarding_completed_by_day_27` and `app_version_at_onboarding_completion`

`client_id` is not unique here, so anything joining this table or its view on `client_id` alone fans out. `retention_clients` collapses it with `MIN(completed_date)` and `GROUP BY client_id` in its `onboarding_completions` CTE; any new consumer has to do the same.

## app_version

The app version the client was running **at the completion**, taken from the earliest completion event on that row's date.

This is deliberately not the same fact as the `app_version` already on `retention_clients`, which is the version as of that row's metric date and moves as the client upgrades. Version-at-completion cannot be recovered from that column, and this table is the only per-completion-event record in the chain, which is why it lives here.

It is `NULL` where the event carried no version. Note it is the version string as the app reports it, not a normalised or numeric form, so it sorts lexicographically — `'100.0'` orders below `'9.0'` — and anything comparing versions has to parse it.

Two caveats inherited from `completed_date`, which is the *reported* day rather than the day the event happened. Within a day the choice is exact, since it orders on `event_timestamp`. Across days it is only as good as the reported date: a client who completed on an old version but whose ping arrived after an upgrade carries the later day's version. `retention_clients` picks the version from the client's earliest *reported* completion day, the same ordering its flag uses, so the two columns always describe the same event.

## Floor

The table will be backfilled from 2025-01-01, so a client whose completions all fall before that date will have no row here.

That matters downstream: on `retention_clients` those clients will read `NULL` rather than `FALSE`, indistinguishable from clients who never completed. Anyone segmenting retention on the flag should filter `first_seen_date >= '2025-01-01'` so the population is limited to clients whose whole life falls inside the window.

## Runs

One scheduled run per day, writing that day's partition. Re-running a date rewrites that date and cannot reach any other day's rows; against unchanged source data it produces the same rows, and it is how rows that landed in the source after that day's run get picked up.
