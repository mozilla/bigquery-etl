WITH combined AS (
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    REPLACE(LOWER(metrics.string.top_sites_advertiser), "o=45:a", "yandex") AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    client_info.app_channel AS release_channel,
    metrics.quantity.top_sites_position AS position,
    IF(metrics.url.top_sites_reporting_url IS NULL, 'remote settings', 'contile') AS provider,
    IF(metrics.string.top_sites_ping_type = "topsites-click", "click", "impression") AS event_type,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.top_sites`
  WHERE
    metrics.string.top_sites_ping_type IN ("topsites-click", "topsites-impression")
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    REPLACE(LOWER(advertiser), "o=45:a", "yandex") AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    position,
    IF(reporting_url IS NULL, 'remote settings', 'contile') AS provider,
    'impression' AS event_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.topsites_impression`
  WHERE
    -- For firefox 116+ use firefox_desktop.top_sites instead
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1836283
    SAFE_CAST(SPLIT(version, ".")[OFFSET(0)] AS INT64) < 116
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    REPLACE(LOWER(advertiser), "o=45:a", "yandex") AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    position,
    IF(reporting_url IS NULL, 'remote settings', 'contile') AS provider,
    'click' AS event_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.topsites_click`
  WHERE
    -- For firefox 116+ use firefox_desktop.top_sites instead
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1836283
    SAFE_CAST(SPLIT(version, ".")[OFFSET(0)] AS INT64) < 116
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    LOWER(metrics.string.top_sites_contile_advertiser) AS advertiser,
    normalized_os,
    'release' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.topsites_impression`
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'phone' AS form_factor,
    normalized_country_code AS country,
    LOWER(metrics.string.top_sites_contile_advertiser) AS advertiser,
    normalized_os,
    'beta' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.topsites_impression`
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'phone' AS form_factor,
    normalized_country_code AS country,
    LOWER(metrics.string.top_sites_contile_advertiser) AS advertiser,
    normalized_os,
    'nightly' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.topsites_impression`
  UNION ALL
  SELECT
    -- Due to the renaming (from 'topsite' to 'topsites'), some legacy Firefox
    -- versions are still using the `topsite` key in the telemetry
    IFNULL(metrics.uuid.top_sites_context_id, metrics.uuid.top_site_context_id) AS context_id,
    DATE(submission_timestamp) AS submission_date,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    IFNULL(
      LOWER(metrics.string.top_sites_contile_advertiser),
      LOWER(metrics.string.top_site_contile_advertiser)
    ) AS advertiser,
    -- This is now hardcoded, we can use the derived `normalized_os` once
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1773722 is fixed
    'iOS' AS normalized_os,
    'release' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.topsites_impression`
  UNION ALL
  SELECT
    -- Due to the renaming (from 'topsite' to 'topsites'), some legacy Firefox
    -- versions are still using the `topsite` key in the telemetry
    IFNULL(metrics.uuid.top_sites_context_id, metrics.uuid.top_site_context_id) AS context_id,
    DATE(submission_timestamp) AS submission_date,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    IFNULL(
      LOWER(metrics.string.top_sites_contile_advertiser),
      LOWER(metrics.string.top_site_contile_advertiser)
    ) AS advertiser,
    -- This is now hardcoded, we can use the derived `normalized_os` once
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1773722 is fixed
    'iOS' AS normalized_os,
    'beta' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.topsites_impression`
),
with_event_count AS (
  SELECT
    *,
    COUNT(*) OVER (
      PARTITION BY
        submission_date,
        context_id,
        event_type,
        form_factor
    ) AS user_event_count,
  FROM
    combined
  ORDER BY
    context_id
)
SELECT
  * EXCEPT (context_id, user_event_count, event_type),
  COUNTIF(event_type = "impression") AS impression_count,
  COUNTIF(event_type = "click") AS click_count,
FROM
  with_event_count
WHERE
  submission_date = @submission_date
  -- Filter out events associated with suspiciously active clients.
  AND NOT (user_event_count > 50 AND event_type = 'click')
  AND IF(
    DATE_DIFF(CURRENT_DATE(), @submission_date, DAY) > 30,
    ERROR("Data older than 30 days has been removed"),
    NULL
  ) IS NULL
GROUP BY
  submission_date,
  form_factor,
  country,
  advertiser,
  release_channel,
  normalized_os,
  position,
  provider
