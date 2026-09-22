# search_derived.search_clients_daily_glean_v1

## Description

A daily aggregate of desktop searches, one row per client per day per engine, partner code and access point. Built on Glean, and runs alongside `search_clients_daily_v8`; the seven `legacy_` columns carry a v8-comparable number from this table alone.

* **Grain:** `client_id`, `submission_date`, `normalized_engine`, `partner_code`, `source`
* **Key fields:** the five grain keys, `sample_id` (the clustering field), the `serp_` ad and engagement measures, `sap_counts_total`, and the seven `legacy_` parity counters
* **Sources:**
  * `mozdata.firefox_desktop.serp_events` — SERP impressions, carrying the ad, engagement and abandonment measures
  * `firefox_desktop_derived.events_stream_v1` — `sap.counts` events, carrying search access point activity
  * `firefox_desktop_stable.metrics_v1` — the seven `legacy_` parity counters, taken from the `browser.search.content`, `withads` and `adclicks` labeled counters, and the ad-blocker add-on flag
  * `revenue.monetization_blocking_addons` — the list of ad-blocking add-ons
* **Filters:** `event = 'sap.counts'` on the SAP side, and enabled monetization-blocking add-ons on the ad-blocker side. Each source is restricted to the run's partition, and a row from any one source reaches the output
* **Downstream:** the view `search.search_clients_daily_glean`, which exposes this table unchanged

The three sides are combined with full outer joins, so a row survives if any of them has activity for that key.

## Reading the columns

**The prefix on a column says which side it can come from.** An unprefixed column is coalesced in the order SERP, SAP, legacy. A `serp_`, `sap_` or `legacy_` prefix marks a column that side supplies alone, so the name tells you which columns read `0` or `null` on a row built without that side.

**Counts and sums fall back to `0`.** A row present on one side only has genuinely no activity on the others for that key. Six columns read `null` on a legacy-only row, and legacy-only keys are roughly a quarter of the table: `is_default_browser` and `sap_overridden_by_third_party`, which the events pings supply; `sap_provider_id` and `sap_provider_name`, from SAP; `serp_ad_click_target` and `serp_ad_blocker_inferred`, from SERP. All six are strings or booleans, where a zero would carry no meaning.

**`partner_code` is always populated.** It is part of the grain, and an absent or empty code resolves to the literal `no_code` on both sides, so splitting on it with `=` and `!=` covers every row.

**`policies_is_enterprise` follows a different aggregation rule on each side.** The pipelines take the mode over the day's events, matching v8; the legacy side publishes the value its winning metrics ping carried. The two diverge where the latest ping carries a null and an earlier one carries a value: the mode skips the null and returns the earlier value, where the legacy rule returns the null.

## Floor

The table reaches back 775 days, the partition expiry, and all three partitioned sources share that floor. It is a rolling window that advances daily, so a backfill range is computed against the run date.

## Runs

One scheduled run per day in the `bqetl_search` DAG, with `date_partition_offset: -1`, so the run for logical date D writes the partition for D-1. The offset aligns this table with `serp_events_v2`, which populates the previous day's partition on each run: each run here reads the SERP partition its upstream run has just written.

A consequence when comparing the two tables: a given day's partition here is produced a day later than `search_clients_daily_v8`'s partition for the same day.

Re-running a date rewrites that date alone.

Every column has a description in `schema.yaml`. Query tests are in `tests/sql/moz-fx-data-shared-prod/search_derived/search_clients_daily_glean_v1/`.
