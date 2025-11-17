[DENG-8178 for search_clients_daily_v9](https://mozilla-hub.atlassian.net/browse/DENG-8178)

A daily aggregate of desktop searches per (client_id, engine, source).

Exposed to users as view `search.search_clients_engines_sources_daily`.

```mermaid
graph TD
    %% OTHER
    adblocker("clients with adblocker<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    %% SAP
    subgraph SG1["SAP: Customer Enrichment Pipeline"]
    base_sap("sap_events_with_client_info<br/>---<br/>latest record by:<br/>client_id<br/>submission_date<br/>normalized_engine<br/>partner_code<br/>source")

    sap_enterprise("sap_is_enterprise<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    sap_full_events("sap_events_clients_adblocker_enterprise")

    sap_agg("sap_aggregates<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>normalized_engine<br/>partner_code<br/>source")

    sap_final("sap_final")

    base_sap -->|"base SAP CTE"| sap_full_events

    sap_enterprise -->|"left join using (client_id, submission_date)"| sap_full_events

    sap_full_events -->|"sap events"| sap_final

    sap_agg -->|"left join using (client_id, submission_date, normalized_engine, partner_code, source)"| sap_final
    end

    adblocker -->|"left join using (client_id, submission_date)"| sap_full_events
    adblocker -->|"left join using (client_id, submission_date)"| serp_full_events

    %% SERP
    subgraph SG2["SERP: Customer Enrichment Pipeline"]
    base_serp("serp_events_with_client_info<br/>---<br/>latest record by:<br/>client_id<br/>submission_date<br/>serp_provider_id<br/>partner_code<br/>serp_search_access_point")

    serp_enterprise("serp_is_enterprise<br/>---<br/>group by:<br/>client_id<br/>submission_date")

    serp_full_events("serp_events_clients_adblocker_enterprise")

    serp_agg("serp_aggregates<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>serp_provider_id<br/>partner_code<br/>serp_search_access_point")

    serp_ad_click("serp_ad_clicks<br/>---<br/>group by:<br/>client_id<br/>submission_date<br/>serp_provider_id<br/>partner_code<br/>serp_search_access_point")

    serp_final("serp_final")

    base_serp -->|"base SERP CTE"| serp_full_events

    serp_enterprise -->|"left join using (client_id, submission_date)"| serp_full_events

    serp_full_events -->|"serp events"| serp_final

    serp_agg -->|"left join using (client_id, submission_date, serp_provider_id, partner_code, serp_search_access_point)"| serp_final

    serp_ad_click -->|"left join using (client_id, submission_date, serp_provider_id, partner_code, serp_search_access_point)"| serp_final
    end

    %% FINALS

    join_sap_serp("join_sap_serp<br/>---<br/>full outer join on:<br/>client_id<br/>submission_date<br/>engine<br/>partner_code<br/>source")

    merged("merged (using max and restructuring arrays)<br/>---<br/>group by:<br/>sap_client_id<br/>serp_client_id<br/>sap_submission_date<br/>serp_submission_date<br/>sap_normalized_engine<br/>serp_provider_id<br/>sap_partner_code<br/>serp_partner_code<br/>sap_search_access_point<br/>serp_search_access_point<br/>")

    sap_final --> join_sap_serp

    serp_final --> join_sap_serp

    join_sap_serp -->|"merge SAP and SERP records into one record"| merged

    %% Styling to match dbt docs
    classDef cteStyle fill:#5c9fd6,stroke:#4a7fb8,stroke-width:2px,color:#fff
    classDef joinStyle fill:#9b6bcc,stroke:#7d4fa8,stroke-width:2px,color:#fff
    classDef intermediateStyle fill:#f4a261,stroke:#e76f51,stroke-width:2px,color:#fff
    classDef finalStyle fill:#81c784,stroke:#66bb6a,stroke-width:2px,color:#fff


    class base_sap,adblocker,sap_enterprise,base_serp,serp_enterprise,sap_agg,serp_agg,serp_ad_click cteStyle
    class sap_full_events,serp_full_events joinStyle
    class sap_final,serp_final intermediateStyle
    class join_sap_serp,merged finalStyle
```

## Tables (CTEs)

### Adblocker

These are the `_adblocker_addons_cte`s. The grain is **one row per `client_id` per `submission_date`**. Each row represents whether a specific client had at least one active ad-blocking add-on on a specific day. So if a client had multiple active ad-blocking add-ons on the same day, they would still only get one row in this result set with `has_adblocker_addon` = true.

Comes from `moz-fx-data-shared-prod.revenue.monetization_blocking_addons` and `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`

The CTE:

- unnests the `metrics.object.addons_active_addons` array to examine each addon
- `inner join`s with the list of known ad-blocking add-ons
- filters to only include addons that are enabled (not user-disabled, app-disabled, or blocklisted)
- sets `has_adblocker_addon` to `true` if any matching ad blocker is found
- groups by `client_id` and `date(submission_timestamp)` (aliased as `submission_date`)

### Enterprise

These are the `_is_enterprise_cte`s. The grain is **one row per `client_id` per `submission_date`**. Each row represents a specific client's most recent enterprise policy status for a specific day. **Note:** If a client had multiple events on the same day, they would still only get one row in this result set, showing their most recent enterprise status for that day (based on the latest `event_timestamp`).

SAP's comes from `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`

SERP's comes from `moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2`

The CTE:

- uses `array_agg()` with `order by event_timestamp desc` to collect all `policies_is_enterprise` values for that client-date combination
- takes the last value (`array_last()`) which, combined with the descending order, gets the **most recent** enterprise policy status based on `event_timestamp`
- casts it to a `boolean`
- groups by `client_id` and `date(submission_timestamp)` (aliased as `submission_date`)

### SAP

`moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`

### SERP

`moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2` and `mozdata.firefox_desktop.serp_events`
