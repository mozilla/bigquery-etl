# search_derived.search_dau_aggregates_glean_v1

## Description

Daily desktop DAU by search behavior, per partner: `dau_w_engine_as_default` and `dau_engaged_w_sap`. The Glean desktop counterpart of the desktop arm of `search_dau_aggregates_v1`, and it runs alongside it.

* **Grain:** `submission_date`, `device`, `partner`, `country`, `normalized_channel`, `distribution_id`, `normalized_default_search_engine`, `normalized_engine`, `sap_category`, `ad_click_category` and `ad_click_category_serp`
* **Sources:**
  * `firefox_desktop.baseline_active_users` — the DAU population, keyed on the Glean `client_id`
  * `search_derived.search_clients_daily_glean_v1` — search activity, and a last fallback for the default engine and distribution
  * `firefox_desktop_stable.metrics_v1` — the default engine and distribution of every DAU client
* **Downstream:** the view `search.search_dau_aggregates_glean`, which exposes this table unchanged

**Desktop only.** The mobile arm of `search_dau_aggregates_v1` already reads Glean, so it is not copied here, and `device` is always `desktop`. Read mobile rows from the legacy table with `device = 'mobile'`. Reading that table without the filter counts desktop twice.

## Reading the columns

**`dau_w_engine_as_default`** counts every DAU client whose default engine is the partner's, whether or not they searched, as `search_dau_aggregates_v1` does. **`dau_engaged_w_sap`** counts DAU clients with a search access point event on the partner's engine. For the Google partner both count only clients in countries eligible for Google. The eligible-country lists are copied unchanged from `search_dau_aggregates_v1`, including the date the list changes.

**Both are distinct-client counts within the row, so they cannot be summed across rows** without double counting. See engine fan-out below.

**Engine fan-out.** A DAU client who searched on more than one engine in a day has a row for each engine, and their default engine counts in each of those rows. Summing `dau_w_engine_as_default` across `normalized_engine` therefore counts them once for each engine. `search_dau_aggregates_v1` behaves the same way, so this matches the legacy table.

**`sap_category`, `ad_click_category` and `ad_click_category_serp` are grain keys.** `sap_category` is 1 when the client had a search access point event. `ad_click_category` is 1 when the client had a tagged ad click in the `browser.search.adclicks` counters, the series comparable to `search_clients_daily_v8`. `ad_click_category_serp` is 1 for a tagged ad click in SERP events. A client can land on a different row depending on which flag is used.

## How the DAU population and attributes are built

**DAU population.** `firefox_desktop.baseline_active_users` with `is_dau`, limited to `Firefox Desktop`, which leaves out MozillaOnline and BrowserStack. It has one row per client-day, so `country` is one value per client-day. Copied profiles give several Glean ids for one legacy id, so this DAU runs higher than `search_dau_aggregates_v1`'s, and per-DAU measures will not tie to their legacy counterparts.

**Search activity joins on `client_id` only,** not on country as `search_dau_aggregates_v1` did. The DAU side and the search side no longer share a geo source, so a country join would count a searcher as a non-searcher wherever their countries differ.

**Default engine and distribution for every DAU client.** `search_clients_daily_glean_v1` only has rows for clients with search activity, so the default engine and `distribution_id` of a client who did not search come from the metrics ping:

* The nearest ping from 7 days before to 1 day after the date is used, nearest day first and the latest ping within a day. Some DAU clients send no metrics ping on a given day, and both values change rarely.
* The default engine is `provider_id`, then `engine_id` for builds that lack it, with an `engine_id` starting with `other` read as a user-installed engine, then the client table.
* `distribution_id` is `client_info.distribution.name`, then the client table. It is NULL for default installs, as in the client table.
* The engine is grouped into Google, Bing or DuckDuckGo by a substring match, and any other engine is NULL. A small share of DAU still has no engine from any source and is counted under no partner.

## Population choices

**The table covers the DAU population only.** Searchers who are not DAU on their search day are dropped. That is deliberate. Use the search tables for the searcher population.

**Bing Acer exclusion is an open decision.** The Bing arm applies the `distribution_id` Acer check, but not the exclusion of clients on the `search.acer_cohort` list, which is keyed on the legacy `client_id`. Bing DAU therefore includes Acer clients the legacy table excluded. This is accepted for now and is to be finalized before the table feeds revenue reporting.

## Known behaviours

**Firefox 138 floor.** `sap_category` and `dau_engaged_w_sap` depend on the `sap.counts` event, which only Firefox 138 and later send, and clients on older versions are essentially absent from the Glean search data. A comparison with the legacy search tables therefore needs to filter both sides to Firefox 138 and later. Without that filter the Glean tables read low against legacy, and by more where many clients are still on older versions.

**SERP ad clicks step up at Firefox 157.** `ad_click_category_serp` is based on SERP ad clicks, which step up from Firefox 157, when the fix for bug 2069054 landed. The counters behind `ad_click_category` do not step. The step stays in the historical data, so a trend across the Firefox 157 rollout shows a level change that is not user behavior.

## Runs

One scheduled run per day in the `bqetl_search_dashboard` DAG, with `date_partition_offset: -1`, so the run for logical date D writes the partition for D-1. The metrics-ping lookback reads one day past the date, which the offset leaves time to land. A rerun before that day has landed can leave more clients without an engine.

Re-running a date rewrites that date alone.
