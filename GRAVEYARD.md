# bigquery-etl Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## Fenix and VR Browser `clients_daily` and `clients_last_seen`

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/895)

We now dynamically discover Glean baseline tables, creating `baseline_clients_daily`
and `baseline_clients_last_seen` tables for each Glean application that sends
baseline pings. We had similar per-app queries defined for Fenix and VR Browser,
but these are now being removed in favor of the generic baseline ping ETL.

There is some loss of functionality here, because we were previously
pulling in some fields from metrics pings; we may want to reference these in
the future if we find need to include metrics or other ping info in this ETL.

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
