# search_derived.search_aggregates_glean_v1

## Description

Daily desktop search activity from `search_clients_daily_glean_v1`, aggregated across unique sets of dimensions with `client_id` removed. The Glean analogue of `search_aggregates_v8`, and it runs alongside it.

* **Grain:** `submission_date` and the dimension columns: `normalized_engine`, `partner_code`, `source`, `country`, `row_country`, `distribution_id`, `is_acer_distribution`, `locale`, `app_version`, `os`, `normalized_os`, `os_version`, `channel`, `normalized_channel`, `is_default_browser`, `policies_is_enterprise`, `default_search_engine_display_name`, `default_private_search_engine_display_name`, `home_region` and `normalized_default_search_engine`
* **Sources:**
  * `search_derived.search_clients_daily_glean_v1` — every measure and every dimension except `is_default_browser`
  * `firefox_desktop.baseline_clients_daily` — `is_default_browser`, where it has a value
* **Downstream:** the view `search.search_aggregates_glean`, which exposes this table unchanged

## Reading the columns

**Both measurement families are carried.** The `serp_` columns and `sap_counts_total` keep the names they have in the client table. The seven `counter_` columns are that table's `legacy_` columns under a new prefix. They come from the `browser.search.content`, `withads` and `adclicks` Glean labeled counters in the metrics ping, and they are the series comparable to `search_aggregates_v8`. Each column's description names the client-table column it renames. The prefix says where the number comes from. It does not say how it is counted: only the ad-click counters are counted from network activity.

**Four counters carry no partner code of their own:** `counter_searches_with_ads_tagged_sum`, `counter_searches_with_ads_organic_sum`, `counter_ad_clicks_tagged_sum` and `counter_ad_clicks_organic_sum`. The client table places each on one `partner_code` per client, engine and source, so they are correct summed across `partner_code` and not when sliced by it.

**`sap_kpi` is `sap_counts_total` without runaway rows.** It leaves out client table rows with 10,000 or more SAP for one client, day, engine, `partner_code` and `source`, which is the cap `search_aggregates_v8` applies. `sap_counts_total` keeps them. The rule lives in the `sap_exclusion_reason` column of the view `search.search_clients_daily_glean`, which this query reads, and is applied to the client table's rows before they are aggregated.

**`client_count` is `COUNT(DISTINCT client_id)` within the row's dimensions.** A client can appear in several rows, so it cannot be summed across rows to give a number of clients.

**`is_acer_distribution` is true when `distribution_id` contains `acer`.** It replaces `is_acer_cohort` in `search_aggregates_v8`, which was true for clients that are not Acer. That column also checked the `search.acer_cohort` list of legacy `client_id`s, which this table does not do, so clients that were on an Acer distribution before 2023 and no longer are show false here.

**Two columns have limited history.** `policies_is_enterprise` is only populated from Firefox 144. `has_adblocker_addon` is unreliable before about 2025-06, and so is `clients_with_adblocker_addon`.

## How dimensions are resolved

A client's rows on one day can disagree, because the client table builds each row from whichever of its SERP, SAP and metrics streams has activity. Grouping by each row's own value would spread one client over several output rows, so each dimension is resolved to one value per client-day before aggregating.

**`country`** is the country on the most rows of the client-day. Ties go to the country whose latest row has the latest `ping_end_time`, then to the country code. `row_country` keeps the country the client table carried on the row itself, which can differ for clients whose location changed during the day, typically a VPN.

**Every other client dimension**, from `is_default_browser` and `app_version` to `distribution_id`, takes its value from the event-backed side first (rows with SERP or SAP activity), then the latest `ping_end_time`, then the value itself, and skips NULLs. `is_default_browser` is only filled on the event-backed side, so it is taken from `firefox_desktop.baseline_clients_daily` when that has a value, because the metrics ping carries none.

`normalized_default_search_engine` is derived from the resolved default search engine provider id.

`os` and `normalized_os` are resolved separately, so in a rare client-day with rows on different operating systems they can come from different rows. `os` is the raw client value and `normalized_os` is the bucketed name (Windows, Mac, Linux or Other).

## Known behaviours

**Firefox 138 floor.** `sap_counts_total` comes from the `sap.counts` event, which only Firefox 138 and later send, and clients on older versions are essentially absent from the Glean search data. A comparison with the legacy search tables therefore needs to filter both sides to Firefox 138 and later. Without that filter the Glean tables read low against legacy, and by more where many clients are still on older versions.

**SERP ad clicks step up at Firefox 157.** The `serp_ad_clicks_*` columns step up from Firefox 157, when the fix for bug 2069054 landed. The `counter_ad_clicks_*` columns do not step, so the two series diverge by version. The step stays in the historical data, so a trend across the rollout, around the start of October 2026, shows a level change that is not user behaviour.

## Runs

One scheduled run per day in the `bqetl_search` DAG, with `date_partition_offset: -1`, so the run for logical date D writes the partition for D-1. A given day's partition here is produced a day later than `search_aggregates_v8`'s partition for the same day, because the client table it reads is.

Re-running a date rewrites that date alone.
