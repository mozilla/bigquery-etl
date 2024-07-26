WITH
-- Firefox for Android
fenix_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.fenix.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Firefox for iOS
firefox_ios_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Firefox Focus for Android
focus_android_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.focus_android.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Firefox Focus for iOS
focus_ios_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.focus_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Firefox Klar for Android
klar_android_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.klar_android.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Firefox Klar for iOS
klar_ios_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.klar_ios.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- Mozilla VPN
mozilla_vpn_baseline AS (
  SELECT
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    metadata.isp.name,
    metadata.isp.organization,
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn.baseline`
  WHERE
    DATE(submission_timestamp) = @submission_date
        -- Country can be null if geoip lookup failed.
        -- There's no point in adding these to the analyses.
        -- Due to a bug in `telemetry.clients_daily` we need to
        -- check for '??' AS well in addition to null.
    AND metadata.geo.country IS NOT NULL
    AND metadata.geo.country <> "??"
),
-- combine baseline pings FROM different products
all_products_baseline AS (
  SELECT
    *
  FROM
    fenix_baseline
  UNION ALL
  SELECT
    *
  FROM
    firefox_ios_baseline
  UNION ALL
  SELECT
    *
  FROM
    focus_android_baseline
  UNION ALL
  SELECT
    *
  FROM
    focus_ios_baseline
  UNION ALL
  SELECT
    *
  FROM
    klar_android_baseline
  UNION ALL
  SELECT
    *
  FROM
    klar_ios_baseline
  UNION ALL
  SELECT
    *
  FROM
    mozilla_vpn_baseline
),
final AS (
  SELECT
    country,
    IF(city IS NULL, "unknown", city) AS city,
    geo_subdivision1,
    geo_subdivision2,
    submission_hour,
    COUNT(*) AS ping_count
  FROM
    all_products_baseline
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    submission_hour
  HAVING
    ping_count > 100
)
SELECT
  *
FROM
  final
