# glean_telemetry_derived.recent_new_profile_churn_clients_v2

## Description

The cohorts too young to be complete in `glean_telemetry_derived.new_profile_churn_clients_v2`, with the same columns. `cohort_cutoff` starts at the day after the newest cohort there, or 30 days before the run date if that table has none, so the two tables never hold the same cohort.

Each run rewrites the whole table, so day and churn flags fill in as the cohorts age, and each cohort leaves once it is written to `new_profile_churn_clients_v2`.

See the `new_profile_churn_clients_v2` README for the grain, sources, flags and how to read the `glean_telemetry.new_profile_churn_clients` view.
