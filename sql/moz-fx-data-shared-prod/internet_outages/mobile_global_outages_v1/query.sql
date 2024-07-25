-- combine baseline pings from different products

with
-- Firefox for Android
fenix_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.fenix.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Firefox for iOS
firefox_ios_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.firefox_ios.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Firefox Focus for Android
focus_android_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.focus_android.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Firefox Focus for iOS
focus_ios_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.focus_ios.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Firefox Klar for Android
klar_android_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.klar_android.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Firefox Klar for iOS
klar_ios_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.klar_ios.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),
-- Mozilla VPN
mozilla_vpn_baseline as (
    select
        metadata.geo.country,
        metadata.geo.city,
        metadata.geo.subdivision1 as geo_subdivision1,
        metadata.geo.subdivision2 as geo_subdivision2,
        TIMESTAMP_TRUNC(submission_timestamp, hour) as submission_hour,
        metadata.isp.name,
        metadata.isp.organization,

    from `moz-fx-data-shared-prod.mozilla_vpn.baseline`
    WHERE
        date(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' as well in addition to null.
        and metadata.geo.country is not null
        and metadata.geo.country <> "??"
),

all_products_baseline as (
    select * from fenix_baseline
    union all select * from firefox_ios_baseline
    union all select * from focus_android_baseline
    union all select * from focus_ios_baseline
    union all select * from klar_android_baseline
    union all select * from klar_ios_baseline
    union all select * from mozilla_vpn_baseline
),

final as (
    select
        country,
        IF(city is null, "unknown", city) as city,
        geo_subdivision1,
        geo_subdivision2,
        submission_hour,
        count(*) as ping_count
    from all_products_baseline
    group by
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        submission_hour
    having ping_count > 100
)

select * from final
