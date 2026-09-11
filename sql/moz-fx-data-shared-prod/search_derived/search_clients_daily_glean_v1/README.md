[DENG-8178](https://mozilla-hub.atlassian.net/browse/DENG-8178)

A daily aggregate of desktop searches, one row per `client_id`, `submission_date`, `normalized_engine`, `partner_code` and `source`.

Exposed to users as view `search.search_clients_engines_sources_daily`.

This is the Glean-based replacement for the legacy `search_clients_daily_v8`.

```mermaid
graph TD
    %% OTHER
    adblocker("clients with adblocker<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    legacy("legacy_parity_counters<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>normalized_engine<br/>partner_code<br/>search_access_point")

    %% SAP
    subgraph SG1["SAP: Customer Enrichment Pipeline"]
    sap_base("sap_base<br/>---<br/>grain keys resolved once:<br/>normalized_engine<br/>partner_code<br/>source")

    sap_events_info("sap_events_with_client_info<br/>---<br/>latest record by:<br/>client_id<br/>submission_date<br/>normalized_engine<br/>partner_code<br/>source")

    sap_enterprise("sap_is_enterprise<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    sap_full_events("sap_events_clients_ad_enterprise")

    sap_agg("sap_aggregates<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>normalized_engine<br/>partner_code<br/>source")

    sap_final("sap_final")

    sap_base -->|"one sap.counts event per row"| sap_events_info

    sap_base -->|"one sap.counts event per row"| sap_agg

    sap_base -->|"one sap.counts event per row"| sap_enterprise

    sap_events_info -->|"one row per grain key"| sap_full_events

    sap_enterprise -->|"left join using (client_id, submission_date)"| sap_full_events

    sap_full_events -->|"sap events"| sap_final

    sap_agg -->|"left join using (client_id, submission_date, normalized_engine, partner_code, source)"| sap_final
    end

    adblocker -->|"left join using (client_id, submission_date)"| sap_full_events
    adblocker -->|"left join using (client_id, submission_date)"| serp_full_events

    %% SERP
    subgraph SG2["SERP: Customer Enrichment Pipeline"]
    serp_base("serp_base<br/>---<br/>grain keys resolved once:<br/>provider_id<br/>partner_code<br/>search_access_point")

    serp_events_info("serp_events_with_client_info<br/>---<br/>latest record by:<br/>client_id<br/>submission_date<br/>provider_id<br/>partner_code<br/>search_access_point")

    serp_enterprise("serp_is_enterprise<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    serp_full_events("serp_events_clients_ad_enterprise")

    serp_agg_base("serp_aggregates_base<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>provider_id<br/>partner_code<br/>search_access_point")

    serp_agg("serp_aggregates<br/>---<br/>flattens ad_components_all<br/>into ad_click_target")

    serp_final("serp_final")

    serp_base -->|"one row per SERP impression"| serp_events_info

    serp_base -->|"one row per SERP impression"| serp_agg_base

    serp_base -->|"one row per SERP impression"| serp_enterprise

    serp_events_info -->|"one row per grain key"| serp_full_events

    serp_enterprise -->|"left join using (client_id, submission_date)"| serp_full_events

    serp_full_events -->|"serp events"| serp_final

    serp_agg_base -->|"one row per grain key"| serp_agg

    serp_agg -->|"left join using (client_id, submission_date, provider_id, partner_code, search_access_point)"| serp_final
    end

    %% FINALS

    join_sources("join_sources<br/>---<br/>serp_final full outer join sap_final<br/>full outer join legacy_parity_counters on:<br/>client_id<br/>submission_date<br/>engine<br/>partner_code<br/>source<br/>---<br/>shared columns coalesced serp first<br/>grain keys coalesced serp, sap, legacy")

    final("final<br/>---<br/>renames to the output schema<br/>no further aggregation")

    sap_final --> join_sources

    serp_final --> join_sources

    legacy -->|"full outer join on the five grain keys"| join_sources

    join_sources -->|"merge the three sources into one record"| final

    %% Styling to match dbt docs
    classDef cteStyle fill:#5c9fd6,stroke:#4a7fb8,stroke-width:2px,color:#fff
    classDef joinStyle fill:#9b6bcc,stroke:#7d4fa8,stroke-width:2px,color:#fff
    classDef intermediateStyle fill:#f4a261,stroke:#e76f51,stroke-width:2px,color:#fff
    classDef finalStyle fill:#81c784,stroke:#66bb6a,stroke-width:2px,color:#fff


    class sap_base,sap_events_info,adblocker,legacy,sap_enterprise,serp_base,serp_events_info,serp_enterprise,sap_agg,serp_agg_base,serp_agg cteStyle
    class sap_full_events,serp_full_events joinStyle
    class sap_final,serp_final intermediateStyle
    class join_sources,final finalStyle
```

## Helper functions

One temporary function turns the string timestamps that Glean emits into dates.

- `local_date_of` returns the date portion of a client-local timestamp string, `substr(ts, 1, 10)` parsed as `%F`. It is applied to `client_info.first_run_date` and `first_run_date`, to the SAP `ping_info.start_time`, and to the SERP `subsession_start_time`.

Every one of those values is written on the client's own calendar with a trailing offset, so the date is what the client's clock said and nothing needs converting. That matters most for `profile_age_in_days`, which subtracts a first run date from an activity date: read one of them in UTC and the subtraction compares two different frames, which puts a client who installed today at an age of -1 for no reason.

The SAP side reads `ping_info.start_time` rather than the `ping_info.parsed_start_time` sitting beside it. Glean ships both, and they differ in type, not in meaning:

| column              | type        | example                         |
| ------------------- | ----------- | ------------------------------- |
| `start_time`        | `STRING`    | `2026-08-13T09:30:18.000+03:00` |
| `parsed_start_time` | `TIMESTAMP` | `2026-08-13 06:30:18 UTC`       |

They are the same moment. But a `TIMESTAMP` is an absolute instant carrying no timezone, so `date()` of one is always a UTC date and there is no way to recover the client's local date from it — `date(ts, tz)` would need a timezone name, and the row has only a numeric offset, inside the string. Only `start_time` still holds the offset, which is why the local date has to be read from it. The two dates disagree on 9.4% of events.

Parsing no time component also means variable precision costs nothing. `subsession_start_time` arrives in four shapes, two of which have no seconds — `2026-08-12T17:10+05:30` — and a pattern written for the other two returns null on them silently.

## Tables (CTEs)

### Adblocker

These are the `_adblocker_addons` CTEs. The grain is **one row per `client_id` per `submission_date`**. Each row represents whether a specific client had at least one active ad-blocking add-on on a specific day. So if a client had multiple active ad-blocking add-ons on the same day, they would still only get one row in this result set with `has_adblocker_addon = true`.

Comes from `moz-fx-data-shared-prod.revenue.monetization_blocking_addons` and `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`

The CTE:

- unnests the `metrics.object.addons_active_addons` array to examine each addon
- `inner join`s with the list of known ad-blocking add-ons
- filters to only include addons that are enabled (not user-disabled, app-disabled, or blocklisted)
- sets `has_adblocker_addon` to `true` if any matching ad blocker is found
- groups by `client_id` and `date(submission_timestamp)` (aliased as `submission_date`)

Because the join onto the SAP and SERP pipelines is a `left join`, a client that never appears here would otherwise be `null`. Both enrichment CTEs wrap the column in `coalesce(..., false)` so a client with no ad blocker reports a confident `false`, matching v8.

### Legacy parity counters

These are the eight `legacy_` CTEs. The grain is **one row per `client_id`, `submission_date`, `normalized_engine`, `partner_code` and `search_access_point`** — the table's own output grain, which is why they join at `join_sources_cte` rather than into either pipeline.

Comes from `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`, the same partition the adblocker CTE already scans. The column sets are disjoint, so this is a second scan and is billed as one.

They produce seven columns: `legacy_tagged_sap`, `legacy_tagged_follow_on`, `legacy_organic`, `legacy_search_with_ads_tagged`, `legacy_search_with_ads_organic`, `legacy_ad_click_tagged` and `legacy_ad_click_organic`.

#### Provenance

They are recorded on the network-observation path, in `SearchSERPTelemetry.observeActivity`, which does not depend on the preconditions of the SERP page scan.

They are native Glean metrics, not a Legacy Telemetry mirror, so they are unaffected by its removal. The `legacy_` prefix refers to v8 parity, not to the collection mechanism.

#### Shape

Each family is a separate labeled counter per access point — 17 access points times three families, 51 metrics. `legacy_base_cte` collects them into one array of `(access_point, family, counter)` structs and `legacy_exploded_cte` unnests it, so the three families become one long row set keyed by access point instead of 51 near-identical `unnest`es.

The label on each counter carries the rest of the key, colon-separated:

| segment | `content`                            | `withads` and `adclicks` |
| ------- | ------------------------------------ | ------------------------ |
| 0       | provider                             | provider                 |
| 1       | `tagged`, `tagged-follow-on`, `organic` | same                  |
| 2       | partner code                         | absent                   |

Segment 0 goes through `udf.normalize_search_engine`, the same normalization the SAP side applies, so the join key cannot drift.

#### Partner code attribution on the ads families

Only `content` labels carry a partner code, so `legacy_content_agg_cte` groups at the full grain while `legacy_ads_agg_cte` groups one key coarser. `legacy_ranked_cte` ranks each key's partner codes by content volume, and only rank 1 receives the ad counts; every other partner code row gets `0`, so a plain `sum` over the table does not double-count. Ties are broken on `partner_code`, so a backfill reproduces the same winner.

Ad activity on a key with no content row keeps the sentinel `partner_code` value `unknown_code`. That value cannot come from either pipeline, both of which resolve to `no_code` or a real code, so those rows are always legacy-only. They are rare.

`follow_on_from_refine_on_incontent_search`, `follow_on_from_refine_on_serp` and `opened_in_new_tab` have no counter, so the seven columns are always `0` on those access points.

#### Client dimensions on legacy-only rows

A key that appears only in the counters has no SAP event and no SERP impression to describe the client, so without this it would publish measures with no `country`, `locale`, operating system, `channel` or anything else. That is not a rare corner: legacy-only keys are roughly a quarter of the table's rows.

`legacy_with_client_info_cte` supplies them. It reads `legacy_base_cte` — the same read the counters use, not a second scan of `metrics_v1` — and reduces it to one row per `client_id` and `submission_date`, keeping the latest ping by `ping_info.seq` with `document_id` as a deterministic tiebreaker. Client-day is the finest grain available, because the metrics ping carries no engine or access point and its counters are interval totals rather than timestamped events. `legacy_parity_counters_cte` then left-joins it, and `clients_with_adblocker_addons_cte`, on `(client_id, submission_date)` — the same shape the two `_is_enterprise_cte`s use. Both are left joins, so a counter key survives even where no metrics-ping row describes the client.

These values are only ever reached where both pipelines are absent, since `join_sources_cte` coalesces SERP, then SAP, then legacy. Where a pipeline has the row, the pipeline's value wins.

Two columns cannot be filled this way and are `null` on a legacy-only row:

- `is_default_browser`, because `usage.is_default_browser` is not sent in the metrics ping.
- `sap_overridden_by_third_party`, because it is a per-search event extra and has no value at client-day grain. It is taken from the SAP side alone, so it is also `null` on a serp-only row.

Three paths differ from the SAP side, which reads the same metrics from the events ping: the submission URLs are under `metrics.url2` rather than `metrics.url`, `profile_group_id` is spelled `legacy_telemetry_profile_group_id`, and `normalized_app_name` is `null` on every row of this ping rather than populated, so it is supplied as the literal `'Firefox'` — the value the SAP side's normalization returns for these same clients, and the only value this table can carry, since it reads one app. `browser_engagement_max_concurrent_tab_count` is already `int64` here, so the legacy arm of that `coalesce` needs no cast where the SAP arm does.

#### How they compare to v8 and to the SERP columns

Each counter has a v8 counterpart — `legacy_tagged_sap` against `tagged_sap`, `legacy_organic` against `organic`, `legacy_search_with_ads_tagged` against `search_with_ads`, `legacy_ad_click_tagged` against `ad_click`, and the organic pairs — and tracks it closely. `legacy_organic` is the exception; see the known gap below.

Two of them differ from their SERP-derived neighbours for definitional reasons rather than coverage. `legacy_search_with_ads_*` counts a SERP with ads when the ad was **served**, observed on the network, where `serp_with_ads_*_count` requires the ad to have been **visible**, so the SERP column is lower by design. `legacy_ad_click_*` counts a click by matching the outgoing request URL against ad-server patterns, and runs materially above `serp_ad_clicks_tagged_count`.

**Known gap on `legacy_organic`.** It runs below v8's `organic` and the residual is not root-caused, so prefer v8 for organic levels until it is. The rest of the family is close. Part of the gap is structural: some v8 organic rows carry no access-point suffix, and a per-access-point counter has nowhere to put them. The remainder is spread proportionally across access points, which is the shape of a coverage difference rather than a definitional one.

### Enterprise

These are the `_is_enterprise_cte`s. The grain is **one row per `client_id` and `submission_date`**. Each row represents a specific client's enterprise policy status for a specific day, taken as the statistical mode (most common value) across that day's events. **Note:** If a client had multiple events on the same day, they would still only get one row in this result set, showing the mode of their enterprise status for that day (ties broken toward the latest `event_timestamp`). This matches v8.

Both CTEs require `document_id is not null`. The SAP one also sees only `event = 'sap.counts'`, but that filter lives in `sap_base` and is inherited rather than applied here.

#### Aggregation

- collects all `policies_is_enterprise` values for that client-date combination (`array_agg()` with `order by event_timestamp` ascending)
- takes the statistical mode as the enterprise policy status (`mozfun.stats.mode_last()`, ties broken toward the latest event), matching v8's `mode_last` behavior

#### Grouping

- groups by `client_id` and `date(submission_timestamp)` (aliased as `submission_date`)

### SAP and SERP CTEs

SAP's comes from `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`, read by `sap_base` and restricted there to `event = 'sap.counts'`. Its client and metric columns are read out of JSON with `json_value` and `json_extract_scalar`.

`sap_base` resolves the three grain keys once, and also `browser_version_info` — a `select` cannot reference an alias from its own list, so computing the struct there is what lets `sap_events_with_client_info` read four fields off one value instead of calling `mozfun.norm.browser_version_info` once per field. That matches the SERP side, where `serp_events_v2` already stores the struct under the same name. Nothing is excluded from the passthrough, because `events_stream_v1` carries none of the seven names `sap_base` adds.

SERP's comes from `mozdata.firefox_desktop.serp_events`, read by `serp_base`, which every other SERP CTE reads in turn. The `mozdata` view rather than `serp_events_v2` because `serp_events_v2` does not carry the aggregated fields such as `num_ads_visible`; the view adds those and drops only `engagements` and `component_impressions`, so it is a superset of what any SERP CTE needs.

`serp_base` resolves the three grain keys once and passes the rest of the row through with `select * except (glean_client_id, partner_code, sap_source)`. The excluded set is the raw form of each resolved key, dropped so that no consumer can key on it by accident — which would produce exactly the silent join miss this CTE exists to prevent. `partner_code` would have to go regardless, since the derived column reuses the source name and leaving both makes every downstream reference ambiguous; BigQuery enforces that one case, and the other two are the same hazard hidden only by the names differing. `search_engine` stays, because `normalize_search_engine` collapses many raw strings into buckets, making the raw engine a different fact rather than the same fact in another spelling. Every SERP CTE after the base then selects the keys as plain columns, which is what makes them impossible to drift apart.

The engine is normalized on both sides through `udf.normalize_search_engine`. On the SAP side the input is `provider_id`, except where `provider_id` is `other`, in which case `provider_name` is normalized instead. On the SERP side the input is `search_engine`.

`sap_base` also projects those two raw extras as `sap_provider_id` and `sap_provider_name`, and they reach the output under those names, so a consumer can see what the engine `case` collapsed — in particular which provider an `other` row actually was. They are prefixed at `sap_base` rather than at the join, which is the one departure from the prefix convention described below: by `join_sources_cte` the name `provider_id` already means the SERP normalized engine, so an unprefixed SAP `provider_id` would collide with it. There is no SERP counterpart to coalesce with, because SERP carries a single provider extra that `serp_base` normalizes into `provider_id` and does not keep raw.

`partner_code` is a grain key, so two searches on the same engine and source with different partner codes on the same day produce two rows, one per code. It is never `null`: both sides derive it as `coalesce(nullif(partner_code, ''), 'no_code')`, so an empty string and an absent key both become the literal `no_code`. This is required rather than cosmetic — `partner_code` keys all four internal joins, the two `_aggregates` left joins and both full outer joins in `join_sources_cte`, and BigQuery's equality never matches `null` to `null`, so a nullable key would silently lose every affected row's aggregates. It also means consumers can split on `partner_code` with `=` and `!=` without a `null` bucket escaping both sides. The expression appears twice, once in `sap_base` and once in `serp_base`. The two are not textually identical, because SAP has to read the value out of JSON with `json_value` first; what must hold is that both produce the same string for the same input, or the join misses.

SAP `source` values are rewritten onto the SERP vocabulary, which the SERP side reads from `sap_source`. `abouthome` becomes `about_home`, `newtab` becomes `about_newtab`, and every remaining hyphen becomes an underscore, so `urlbar-handoff`, `urlbar-searchmode` and `urlbar-persisted` become `urlbar_handoff`, `urlbar_searchmode` and `urlbar_persisted`. A `null` source stays `null`. The expression appears once, in `sap_base`.

The SERP side lowercases `sap_source`. `serp_events` emits one mixed-case value, `follow_on_from_refine_on_SERP`, in an otherwise entirely lowercase vocabulary, and this column is both a grain key and the output `source` column. Left raw it would be a silent-empty-result trap: a consumer filtering `source = 'follow_on_from_refine_on_serp'` would match nothing, with no error and no hint. Lowering has no effect on the join — the SAP side has no lowercase counterpart for that value, so the row is SERP-only either way, which is correct, since a follow-on search is issued from the results page rather than a browser search access point. The `lower()` appears once, in `serp_base`.

#### Events with client info

These are the `_events_with_client_info` CTEs. The grain is **one row per `client_id`, `submission_date`, engine, `partner_code` and access point** — `normalized_engine` and `source` on the SAP side, `provider_id` and `search_access_point` on the SERP side.

Each row represents the most recent search event for a specific client, on a specific day, using a specific search engine, with a specific partner code, from a specific source. Every other column is a passthrough from that one surviving event.

**Note:** If a client performed multiple searches on the same day with the same engine, partner code and source combination, they would still only get one row in this result set, showing the details from their most recent search event (based on the latest `event_timestamp`).

##### Qualify

- partitions events by `client_id`, `submission_date`, engine, `partner_code` and access point (`row_number()`)
- lists the most recent event first (`order by event_timestamp desc`)
- on the SERP side adds `impression_id` as a secondary sort key so ties are broken deterministically
- keeps only the most recent event (`qualify row_number() = 1`)

#### Events with client, ad blocker and enterprise info

- `_events_with_client_info`
- `left join`ed to `adblocker_addons`, with `has_adblocker_addon` wrapped in `coalesce(..., false)`
- `left join`ed to `is_enterprise_cte`
- using `client_id, submission_date`

#### Aggregates

These are the `_aggregates` CTEs. The grain is **one row per `client_id`, `submission_date`, engine, `partner_code` and access point** — the same keys the corresponding `_events_with_client_info` CTE partitions by.

The SERP side is split in two. `serp_aggregates_base` does the grouping and carries `ad_components_all`, the group's `ad_components` arrays concatenated with `array_concat_agg`. `serp_aggregates` reads that CTE — not `serp_events`, so there is no second scan of the source — and flattens the array into `ad_click_target`, a comma-separated string of the distinct ad components the client clicked, ordered so the concatenation is deterministic (`string_agg(distinct ... order by ...)`). A key whose `ad_components` are all empty concatenates to an empty array, which unnests to no rows and leaves `ad_click_target` null.

The split exists because the flatten and the aggregation cannot share a query level: BigQuery rejects an aggregate inside `unnest`, so `array_concat_agg` has to land as a column before anything unnests it. Concatenating arrays is what makes this safe to compute alongside the counts — a `cross join unnest(ad_components)` in the same CTE would multiply rows and corrupt `count(*)` and every `sum`.

Each row represents aggregated search activity and engagement metrics for a specific client, on a specific day, using a specific search engine, with a specific partner code, from a specific source.

**Note:** If a client performed multiple searches on the same day with the same engine, partner code and source combination, they would get one row in this result set with all their activity aggregated together.

The two sides do not compute the same measures.

- SAP produces `sap_counts_total` (a count of `sap.counts` events) and `concurrent_tab_count_max`, and derives `profile_age_in_days` from `ping_info.start_time` against the first run date.
- SERP produces `counts_total` and the ad measures: tagged and organic search counts, searches with ads, ad clicks, and the `num_ads_*` family. All of them are coined here without the `serp_` prefix and pick it up at the join. Tagged and organic are split on `is_tagged`, and follow-on searches are those whose `search_access_point` is `follow_on_from_refine_on_incontent_search` or `follow_on_from_refine_on_serp`. SERP derives `profile_age_in_days` from `subsession_start_time` against the first run date.

#### SAP and SERP Final

- `_events_clients_ad_enterprise`
- `left join`ed to `_aggregates`

SAP joins using `client_id, submission_date, normalized_engine, partner_code, source`. SERP joins using `client_id, submission_date, provider_id, partner_code, search_access_point`.

### Join the sources

This is the `join_sources_cte`. The grain is **one row per `client_id`, `submission_date`, engine, `partner_code` and access point**, carrying the SAP, SERP and legacy measures for that combination.

#### Full outer join, not serp-driven

Three sides are combined with `full outer join`s, so a row survives if it appears on any of them.

- A search access point with no matching SERP impression keeps its `sap_counts_total` value. Driving the join from SERP alone would drop that activity entirely, since the SAP `source` and SERP `sap_source` vocabularies only partly overlap.
- A SERP impression with no matching SAP event keeps its ad and engagement measures.
- A legacy parity key with no match on either pipeline keeps its counters. A client can increment `browser.search.adclicks` on a page whose SERP impression never registered, so such keys occur.
- Two SERP-only columns are `null` on a sap-only row: `ad_click_target` and `ad_blocker_inferred`. Every other SERP-only measure is a count and falls back to `0`.
- Three SAP-only columns are `null` on a serp-only row: `sap_provider_id`, `sap_provider_name` and `sap_overridden_by_third_party`. None is a count, so there is nothing to zero-fill. The first two have no SERP counterpart at all, the SERP side carrying a single provider extra that `serp_base` normalizes into `provider_id`. The third is a deliberate choice rather than an absence — see the note below.
- A legacy-only row carries its client dimensions from the metrics ping, via `legacy_with_client_info_cte` — see "Client dimensions on legacy-only rows" above. One column is `null` there and nowhere else: `is_default_browser`. The SERP-only and SAP-only measures still zero-fill on these rows, as they do on any row missing that side.

#### Column precedence

Every column present on more than one side is combined with a `coalesce` in the order SERP, SAP, legacy. SERP takes precedence, SAP fills in where the SERP value is `null`, and legacy fills in where neither pipeline carries the row at all. `experiments` is the one exception: SERP arrives as a repeated field and is never `null`, so an empty SERP array would always beat a populated SAP one. It is wrapped in `if(array_length(...) = 0, null, ...)` first, which makes the precedence "whichever side recorded enrollments" rather than "whichever side exists". This applies to the join keys, to the client dimensions such as `country`, `locale` and the operating system columns, to the default and private search engine columns, and to the two shared measures, `profile_age_in_days` and `max_concurrent_tab_count_max` — of which only the second takes a legacy argument.

The prefix on a column name says which side it can come from. A coalesced column has no prefix. A `serp_`, `sap_` or `legacy_` prefix that survives into this CTE means the value exists on that side only — `sap_counts_total`, `sap_provider_id` and `sap_provider_name` from SAP, `serp_counts_total`, `serp_ad_click_target`, `serp_ad_blocker_inferred` and the SERP ad and engagement counts from SERP, and the seven `legacy_` counters from the metrics ping. The `legacy_` ones keep their prefix in the output, as do `serp_counts_total`, `serp_searches_organic_count`, the `sap_provider` pair and `sap_overridden_by_third_party`; every other prefix is stripped in `final_cte`. The prefix distinguishes each counter from the SERP-derived measure of the same events, which is published under its own name.

Every prefix is applied here, at the join, and the side-only column is coined unprefixed upstream. `sap_provider_id` and `sap_provider_name` are the one exception: they carry the prefix from `sap_base` onward, because `provider_id` is already taken by the SERP normalized engine in this CTE and the unprefixed pair would collide with it.

**Coalescing the join keys is load-bearing, not cosmetic.** The final CTE reads every identity column from the SERP side, so without the `coalesce` a sap-only row would emit a `null` `submission_date`, `client_id`, `source`, `country` and `sample_id`. `submission_date` is the fatal one: the table is day-partitioned on it with `require_partition_filter: true`, so those rows would land in the `__NULL__` partition and be unreachable to any query that filters by date — which is every query. `sample_id` matters too, since it is the clustering field.

**On the five grain keys and `sample_id`, the legacy argument is load-bearing for a second reason.** A legacy-only row is reachable by construction — `legacy_parity_counters_cte` assigns the sentinel `unknown_code` to orphan ad rows, a value neither pipeline can produce — so leaving legacy out of the coalesce would publish rows with a wholly `null` grain and populated counters, all of them colliding on one grain tuple. No key becomes nullable in the process: legacy's `partner_code` is the sentinel rather than `null`, and its `submission_date` and `search_access_point` come from the partition filter and a fixed list.

**Anything derived from a published column is derived after the coalesce.** `os_version_major` and `os_version_minor` are computed in `final_cte` from the coalesced `os`, `os_version` and `windows_build_number`, so a row's derived value and the inputs it publishes always come from the same side. Deriving per side and coalescing the two results separately breaks that, because each column then picks its winner independently: a row can publish one side's `windows_build_number` beside a release name the other side computed without it, and nothing in the row says so.

#### Counts and sums are zero, never null

Every count and sum in this CTE falls back to `0`. A row that reaches this CTE from one source only has no counterpart on the others for that client, date, engine, partner code and access point, so zero is the count rather than an unknown.

The trade-off is that a zero no longer distinguishes "no activity" from "the other side's data is missing or late". Conditions where SAP is recorded but not SERP are mostly on engines where we have not instrumented SERP metrics at all. If needed, look at the search provider to check uncertainty.

- `max_concurrent_tab_count_max`, the one shared measure that zero-fills, takes a third `coalesce` argument.
- `sap_counts_total` is zero on a serp-only row.
- The fifteen SERP-only counts are zero on a sap-only row: `serp_counts_total`, the tagged, organic and follow-on search counts, the searches-with-ads and ad-click counts, and the six `num_*` measures.
- The seven `legacy_` counters are zero where the metrics ping carried nothing for that key, which is every row whose key exists on a pipeline but not in the counters.

Six columns are deliberately left alone. `profile_age_in_days` is not a count, and a zero would read as a profile created that day rather than as a missing value. `ad_click_target` is a string and `ad_blocker_inferred` is a boolean, so neither has a meaningful zero. `sap_provider_id`, `sap_provider_name` and `sap_overridden_by_third_party` are the same for strings and booleans, and are the three of the six that are `null` on a serp-only row rather than a sap-only one.

#### Two constraints worth knowing before editing this CTE

- **Both joins use plain equality on every key**, with no both-`null` branches. No grain key is ever `null`, and two `null`s are not a key match: a both-`null` branch would pair every `null`-keyed row on one side with every `null`-keyed row on the other, a cartesian product where plain equality leaves them one-sided instead. Note also that BigQuery requires at least one literal `=` in a `full outer join` ON clause, so `is not distinct from` on its own is rejected whatever the null semantics. The legacy join's right-hand side is the already-coalesced SAP-and-SERP key, `coalesce(serp, sap)`, not either side alone.
- **One `coalesce` needs an explicit cast.** `sap_aggregates_cte` casts its integer counter to `float64`, so combining it with the SERP side would widen the result and break the `INTEGER` type declared in `schema.yaml`. `max_concurrent_tab_count_max` therefore casts the SAP side back to `int64` inside the `coalesce`.

### Final

This is `final_cte`. It renames the joined columns to the output schema and performs no further aggregation or filtering, so the row count matches `join_sources_cte` exactly. There is no `where` clause, which is what allows SAP-only rows to reach the table.

A few output columns are worth calling out.

- `normalized_engine` is the only engine column. v8's `engine` is dropped. In v8 `engine` held the raw engine string and `normalized_engine` was always null; in glean_v1 the engine is normalized through `udf.normalize_search_engine` on both pipelines, so the two columns would have held the same value.
- `sap_provider_id` and `sap_provider_name` are the raw SAP extras behind `normalized_engine`, kept so the normalization is auditable — `normalized_engine` buckets many raw providers into one value, and where `sap_provider_id` is `other` it is `sap_provider_name` that determined the bucket. Both keep their prefix in the output and are `null` on serp-only rows. They have no v8 counterpart.
- `tagged_serp` is the only tagged count. v8's `tagged_sap` is dropped: SAP has no `is_tagged`, so glean_v1 has no independent SAP-side measure to put there and both columns would have carried the same `serp_searches_tagged_count` value.
- `sap_counts_total` is not v8's `sap`, despite measuring the same activity. v8's was a `sum` of the legacy `search_counts.count` counter; this is a `count(*)` over `sap.counts` events, at a grain that also includes `partner_code`. Summing this column will not reproduce v8's `sap`.
- `ping_start_time` and `ping_end_time` are raw client-local strings, not timestamps, and the two sides fill them from different fields — `ping_info.start_time` and `ping_info.end_time` on SAP, `subsession_start_time` and `subsession_end_time` on SERP. The format is not uniform either: the SERP field arrives in four shapes, two of them with no seconds component, so a pattern written for the other two returns `null` on them silently. Anyone parsing these downstream meets both traps this table already hit — that silent null, and the local-offset conversion behind the `first_run_date` off-by-one.
- The ad measures are renamed so each name states which half it counts, rather than leaving the tagged half as the unmarked default the way v8 did. v8's `ad_click` is `ad_click_total`, `ad_clicks_tagged` is `ad_click_tagged`, and `search_with_ads` is `search_with_ads_tagged`. The organic counterparts, `ad_click_organic` and `search_with_ads_organic`, keep their v8 names. So the tagged and organic pairs now read `ad_click_tagged`/`ad_click_organic` and `search_with_ads_tagged`/`search_with_ads_organic`, with `ad_click_total` counting every ad click regardless of `is_tagged`. Note that `ad_click_total` is not guaranteed to equal `ad_click_tagged` plus `ad_click_organic`: the two halves are split on `is_tagged is true` and `is_tagged is false`, both of which exclude `null`, while the total sums every row. They agree only where `is_tagged` is never `null`.
- Three columns are renamed from their v8 names to match the Glean fields they carry: `user_pref_browser_search_region` is `home_region`, and `default_search_engine` and `default_private_search_engine` are `default_search_engine_display_name` and `default_private_search_engine_display_name`. The display-name pair now reads like its own siblings, since `_load_path`, `_partner_code`, `_provider_id`, `_submission_url` and `_overridden` all already published under their full names. The rename is also a warning: a display name is a label rather than an identity, so use `default_search_engine_provider_id` to identify an application-provided engine — a third-party engine reports `other` there.
- `sap_overridden_by_third_party` is taken from the SAP side alone, by decision rather than necessity. Both events define the extra — `sap.counts` on one side, the SERP `impression` on the other — but only SAP populates it, and the two would not be reporting the same occasion anyway: one fires when a search is issued from a browser access point, the other when a results page is shown. Publishing one side keeps the value attributable, at the cost of ignoring the SERP extra if it ever starts arriving.
- `tagged_follow_on` either equals `tagged_serp` or is zero, never anything between. The access point is part of the grain, so within a row either every impression was reached by refining a search from the results page or none was. Summing it beside `tagged_serp` double-counts the follow-on rows.
- `ad_blocker_inferred` and `has_adblocker_addon` sound like the same fact and are not. The first is inferred from the results page and is true only where every one of the row's impressions showed the signs; the second reports an enabled ad-blocking add-on installed on the client that day, and is `false` rather than `null` where none was found.
- `experiments` prefers the SERP passthrough, which arrives as a repeated field and is never `null`. Because `coalesce` returns the first non-`null` argument and an empty array is not `null`, the SAP side is only reached on sap-only rows. The SAP side builds the same shape from JSON, one element per enrollment, ordered by experiment slug.
- The seven `legacy_` columns measure the same events as SERP-derived columns already in the table: `legacy_tagged_sap` against `tagged_serp`, `legacy_ad_click_tagged` against `ad_click_tagged`, `legacy_search_with_ads_tagged` against `search_with_ads_tagged`, and the organic counterparts. The two sets are collected by different mechanisms and do not agree. See the legacy parity counters section above.

glean_v1 is **not** a column-for-column match of v8 and is not intended to be. Every column carries real data; nothing is emitted as a placeholder `null` purely to preserve the v8 shape. Nineteen v8 columns that had no Glean source are therefore absent from both the query and `schema.yaml`: `addon_version`, `search_cohort`, `subsessions_hours_sum`, `active_addons_count_mean`, `unknown`, `is_sap_monetizable`, and the thirteen `scalar_parent_urlbar_searchmode_*` columns.

## Determinism and reproducibility

One output carries a tie that BigQuery does not break. It matches `search_clients_daily_v8` and is left unchanged.

- `policies_is_enterprise` (the `_is_enterprise_cte`s): `mozfun.stats.mode_last(array_agg(... order by event_timestamp))` takes the statistical mode, breaking ties toward the latest event. On two mode-tied values whose events share the same `event_timestamp` the tiebreak is arbitrary. v8 is the same: `mozfun.stats.mode_last(array_agg(... order by submission_timestamp))`, no secondary tiebreaker.

Four outputs were non-deterministic and have been made reproducible:

- `ad_click_target` (`string_agg`): `order by ad_component.component`. `distinct` fixes the set; the `order by` fixes the concatenation order.
- `experiments` on the SAP side (`sap_events_with_client_info_cte`): the array is built by iterating `json_keys(experiments, 1)`, whose order BigQuery does not document as stable, so the element order is pinned with `order by k` on the experiment slug. The SERP side is a passthrough of `ping_info.experiments` and keeps whatever order the ping carried; the two conventions never mix, because the `coalesce` in `join_sources_cte` takes one side's array whole.
- SERP passthrough columns (`serp_events_with_client_info_cte`): the `qualify row_number() over (... order by event_timestamp desc)` picked an arbitrary row when two events for the same client, date, engine, and access point shared an `event_timestamp`. `impression_id` is now a secondary sort key; it is unique in `serp_events_v2`, so the surviving row is fully determined.
- SAP passthrough columns (`sap_events_with_client_info_cte`): the same problem, and the exact mirror of the case above. `event_timestamp` on `events_stream_v1` is itself derived — it prefers a `glean_timestamp` extra and otherwise falls back to `ping_info.parsed_start_time` plus the event's millisecond offset — so two `sap.counts` events in one ping tie whenever they land in the same millisecond. `event_id` is now a secondary sort key; it is `document_id` plus the event's position in the ping, so it is unique per event and the surviving row is fully determined.
