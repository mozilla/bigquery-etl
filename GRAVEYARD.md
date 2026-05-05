# bigquery-etl Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## 2021-12 ASN aggregates

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/2580)
- [Bug](https://mozilla-hub.atlassian.net/browse/DSRE-197)

This dataset was no longer being actively used, and removing it allows us to
limit airflow's access to `payload_bytes_raw`.

## 2021-09 Remove document sampling queries

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/2389)
- [Bug](https://bugzilla.mozilla.org/show_bug.cgi?id=1731777)

We've removed the CI task from mozilla-pipeline-schemas that used this
document sample, so there is no further need for the ETL to support it.

## 2021-08 Remove amplitude views

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/2279)
- [DAG removal PR](https://github.com/mozilla/telemetry-airflow/pull/1328)

We no longer send data to Amplitude, so these views and scripts were
no longer being used.

## 2021-05 attitudes_daily

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/2003)
- [DAG removal PR](https://github.com/mozilla/telemetry-airflow/pull/1299)
- [Bug 1707965](https://bugzilla.mozilla.org/show_bug.cgi?id=)

This pipeline was no longer being actively used. There may be need for
a similar pipeline for an upcoming survey, so this removed code can
serve as a useful reference in that effort.

## 2021-03 Account Ecosystem Telemetry (AET) derived tables

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/1894)

AET was never released except for a short test in the beta population,
and now the project has been decommissioned, so there is no longer
any need for these derived tables.

## 2020-12 Deviations

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/2005)
- [DAG Removal PR](https://github.com/mozilla/bigquery-etl/pull/1637)
- [Blog Post](https://blog.mozilla.org/data/2020/03/30/opening-data-to-understand-social-distancing/)

The `deviations_v1` table was used to understand the change of Firefox
desktop usage during Covid-19 pandemic in 2020. The data is no longer being
actively used.

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
