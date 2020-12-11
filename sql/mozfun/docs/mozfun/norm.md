# norm

Functions for normalizing data.


## glean_baseline_client_info (UDF)

Accepts a glean client_info struct as input and returns a modified struct that includes a few parsed or normalized variants of the input fields.



## glean_ping_info (UDF)

Accepts a glean ping_info struct as input and returns a modified struct that includes a few parsed or normalized variants of the input fields.



## product_info (UDF)

Returns a normalized `product` name and a `canonical_name` for a product
based on the raw `app_name` and `normalized_os` values that appear in pings.

The returned `product` name is intended to be readable and unambiguous, but
short and easy to type. This value is suitable for use as a key in derived
tables.

The returned `canonical_name` is more verbose and is suited for displaying
in visualizations.

The returned struct also contains boolean `contributes_to_2020_kpi` as the
canonical reference for whether the given application is included in KPI
reporting. Additional fields may be added for future years.

The `normalized_os` value that's passed in should be the top-level
`normalized_os` value present in any ping table or you may want
to wrap a raw value in `mozfun.norm.os`
like `mozfun.norm.product_info(app_name, mozfun.norm.os(os))`.

For legacy telemetry pings like `main` ping for desktop and `core` ping for
mobile products, `app_name` should come from the submission URI (stored as
`metadata.uri.app_name` in BigQuery ping tables).

For Glean pings, the concept of an `app_name` doesn't exist, since pings
from different applications are routed to different BigQuery datasets.
Instead, the `app_name` send in for Glean pings should be the same value as
what's expected for `product`. So, a view on top of pings from Fenix should
pass in "Fenix" for `app_name`.

This function also tolerates passing in a `product` value as `app_name` so
that this function is still useful for derived tables which have thrown away
the raw `app_name` value.


The mappings are as follows:

app_name         | normalized_os | product          | canonical_name              | 2019  | 2020
---------------- | ------------- | ---------------- | --------------------------- | ----- | ----
Firefox          | *             | Firefox          | Firefox for Desktop         | true  | true
Fenix            | Android       | Fenix            | Firefox for Android (Fenix) | true  | true
Fennec           | Android       | Fennec           | Firefox for Android (Fennec)| true  | true
Firefox Preview  | Android       | Firefox Preview  | Firefox Preview for Android | true  | true
Fennec           | iOS           | Firefox iOS      | Firefox for iOS             | true  | true
FirefoxForFireTV | Android       | Firefox Fire TV  | Firefox for Fire TV         | false | false
FirefoxConnect   | Android       | Firefox Echo     | Firefox for Echo Show       | true  | true
Zerda            | Android       | Firefox Lite     | Firefox Lite                | true  | true
Zerda_cn         | Android       | Firefox Lite CN  | Firefox Lite (China)        | false | false
Focus            | Android       | Focus Android    | Firefox Focus for Android   | true  | true
Focus            | iOS           | Focus iOS        | Firefox Focus for iOS       | true  | true
Klar             | Android       | Klar Android     | Firefox Klar for Android    | false | false
Klar             | iOS           | Klar iOS         | Firefox Klar for iOS        | false | false
Lockbox          | Android       | Lockwise Android | Lockwise for Android        | true  | true
Lockbox          | iOS           | Lockwise iOS     | Lockwise for iOS            | true  | true
FirefoxReality*  | Android       | Firefox Reality  | Firefox Reality             | false | false


## os (UDF)

Normalize an operating system string to one of the three major desktop
platforms, one of the two major mobile platforms, or "Other".

This is a reimplementation of 
[logic used in the data pipeline](
  <https://github.com/mozilla/gcp-ingestion/blob/a6928fb089f1652856147c4605df715f327edfcd/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L52-L74)>
  to populate `normalized_os`.




## metadata (UDF)

Accepts a pipeline metadata struct as input and returns a modified struct that includes a few parsed or normalized variants of the input metadata fields.



## fenix_app_info (UDF)

Returns canonical, human-understandable identification info for Fenix sources.

The Glean telemetry library for Android by design routes pings based
on the Play Store appId value of the published application.
As of August 2020, there have been 5 separate Play Store appId
 values associated with different builds of Fenix,
 each corresponding to different datasets in BigQuery,
and the mapping of appId to logical app names
(Firefox vs. Firefox Preview) and channel names
 (nightly, beta, or release) has changed over time; see the
[spreadsheet of naming history for Mozilla's mobile browsers](
  <https://docs.google.com/spreadsheets/d/18PzkzZxdpFl23__-CIO735NumYDqu7jHpqllo0sBbPA/edit#gid=0).>

This function is intended as the source of truth for how to map a
specific ping in BigQuery to a logical app names and channel.
It should be expected that the output of this function may evolve
over time. If we rename a product or channel, we may choose to
update the values here so that analyses consistently get the new name.

The first argument (`app_id`) can be fairly fuzzy; it is tolerant
of actual Google Play Store appId values like 'org.mozilla.firefox_beta'
(mix of periods and underscores) as well as BigQuery dataset names
with suffixes like 'org_mozilla_firefox_beta_stable'.

The second argument (`app_build_id`) should be the value
in client_info.app_build.

The function returns a `STRUCT` that contains the logical `app_name`
and `channel` as well as the Play Store `app_id` in the canonical
form which would appear in Play Store URLs.

Note that the naming of Fenix applications changed on 2020-07-03,
so to get a continuous view of the pings associated with a logical
app channel, you may need to union together tables from multiple
BigQuery datasets. To see data for all Fenix channels together, it
is necessary to union together tables from all 5 datasets.
For basic usage information, consider using
`telemetry.fenix_clients_last_seen` which already handles the
union. Otherwise, see the example below as a template for how
construct a custom union.

Mapping of channels to datasets:

- release: `org_mozilla_firefox`
- beta: `org_mozilla_firefox_beta` (current) and `org_mozilla_fenix`
- nightly: `org_mozilla_fenix` (current), `org_mozilla_fennec_aurora`,
and `org_mozilla_fenix_nightly`


```sql
-- Example of a query over all Fenix builds advertised as "Firefox Beta"
CREATE TEMP FUNCTION extract_fields(app_id STRING, m ANY TYPE) AS (
  (
    SELECT AS STRUCT
      m.submission_timestamp,
      m.metrics.string.geckoview_version,
      mozfun.norm.fenix_app_info(app_id, m.client_info.app_build).*
  )
);

WITH base AS (
  SELECT
    extract_fields('org_mozilla_firefox_beta', m).*
  FROM
    org_mozilla_firefox_beta.metrics AS m
  UNION ALL
  SELECT
    extract_fields('org_mozilla_fenix', m).*
  FROM
    org_mozilla_fenix.metrics AS m
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  geckoview_version,
  COUNT(*)
FROM
  base
WHERE
  app_name = 'Fenix'  -- excludes 'Firefox Preview'
  AND channel = 'beta'
  AND DATE(submission_timestamp) = '2020-08-01'
GROUP BY
  submission_date,
  geckoview_version
```
