# bigquery-etl Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## 2021-03 Account Ecosystem Telemetry (AET) derived tables

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/1894)

AET was never released except for a short test in the beta population,
and now the project has been decommissioned, so there is no longer
any need for these derived tables.

## 2020-04 Fenix baseline_daily and clients_last_seen

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/925)

We are now using dynamically generated queries for generic Glean
ETL on top of baseline pings, so we have deprecated previous versions
of daily and last_seen tables.

## Smoot Usage v1

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/460)

The `smoot_usage_*_v1*` tables used a python file to generate the desktop,
nondesktop, and FxA variants, but have been replaced by v2 tables that make
some different design decisions. One of the main drawbacks of v1 was that
we had to completely recreate the final `smoot_usage_all_mtr` table for all
history every day, which had started to take on order 1 hour to run. The
v2 tables instead define a `day_0` view and a `day_13` view and relies on
the Growth and Usage Dashboard (GUD) to query them separately and join the
results together at query time.

## Shredder support for per-cluster deletes

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/733)

For `telemetry_stable.main_v4` shredder used `SELECT` statements over single
clusters, then combined the result to remove rows from the table. This was an
attempt to improve performance so that reserved slots would be cheaper than
on-demand pricing, but it turned out to be slower than using `DELETE`
statements for whole partitions.
