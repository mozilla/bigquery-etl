WITH combined AS (
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'suggest' AS source,
    'impression' AS event_type,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    CASE
    WHEN
      request_id IS NULL
    THEN
      'remote settings'
    ELSE
      'merino'
    END
    AS provider,
    match_type,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    (
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online'
    ) AS suggest_data_sharing_enabled,
  FROM
    contextual_services.quicksuggest_impression
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'suggest' AS source,
    'click' AS event_type,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    CASE
    WHEN
      request_id IS NULL
    THEN
      'remote settings'
    ELSE
      'merino'
    END
    AS provider,
    match_type,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    (
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online'
    ) AS suggest_data_sharing_enabled,
  FROM
    contextual_services.quicksuggest_click
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    'impression' AS event_type,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    CASE
    WHEN
      reporting_url IS NULL
    THEN
      'remote settings'
    ELSE
      'contile'
    END
    AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    contextual_services.topsites_impression
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    'click' AS event_type,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    CASE
    WHEN
      reporting_url IS NULL
    THEN
      'remote settings'
    ELSE
      'contile'
    END
    AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    contextual_services.topsites_click
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    metrics.string.top_sites_contile_advertiser AS advertiser,
    'release' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    org_mozilla_firefox.topsites_impression
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
    'phone' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    metrics.string.top_sites_contile_advertiser AS advertiser,
    'beta' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    org_mozilla_firefox_beta.topsites_impression
  UNION ALL
  SELECT
    metrics.uuid.top_sites_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
    'phone' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    metrics.string.top_sites_contile_advertiser AS advertiser,
    'nightly' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    org_mozilla_fenix.topsites_impression
  UNION ALL
  SELECT
    -- Due to the renaming (from 'topsite' to 'topsites'), some legacy Firefox
    -- versions are still using the `topsite` key in the telemetry
    IFNULL(metrics.uuid.top_sites_context_id, metrics.uuid.top_site_context_id) AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    IFNULL(
      metrics.string.top_sites_contile_advertiser,
      metrics.string.top_site_contile_advertiser
    ) AS advertiser,
    'release' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    -- This is now hardcoded, we can use the derived `normalized_os` once
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1773722 is fixed
    'iOS' AS normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    org_mozilla_ios_firefox.topsites_impression
  UNION ALL
  SELECT
    -- Due to the renaming (from 'topsite' to 'topsites'), some legacy Firefox
    -- versions are still using the `topsite` key in the telemetry
    IFNULL(metrics.uuid.top_sites_context_id, metrics.uuid.top_site_context_id) AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    IF(events[SAFE_OFFSET(0)].name = 'contile_click', 'click', 'impression') AS event_type,
    -- The adMarketplace APIs accept form factors out of "desktop", "phone", or "tablet";
    -- we are currently always using "phone" for Fenix, so stay consistent with that here.
    'phone' AS form_factor,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    IFNULL(
      metrics.string.top_sites_contile_advertiser,
      metrics.string.top_site_contile_advertiser
    ) AS advertiser,
    'beta' AS release_channel,
    SAFE_CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    -- Only Contile is available for mobile tiles.
    'contile' AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
    -- This is now hardcoded, we can use the derived `normalized_os` once
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1773722 is fixed
    'iOS' AS normalized_os,
    -- 'suggest_data_sharing_enabled' is only available for `quicksuggest_*` tables
    NULL AS suggest_data_sharing_enabled,
  FROM
    org_mozilla_ios_firefoxbeta.topsites_impression
),
with_event_count AS (
  SELECT
    *,
    COUNT(*) OVER (
      PARTITION BY
        submission_date,
        context_id,
        source,
        event_type,
        form_factor
    ) AS user_event_count,
  FROM
    combined
  ORDER BY
    context_id
), 
contextual_service_events AS (
  SELECT
    * EXCEPT (context_id, user_event_count),
    COUNT(*) AS event_count,
    COUNT(DISTINCT(context_id)) AS user_count,
  FROM
    with_event_count
  WHERE
    submission_date = @submission_date
    -- Filter out events associated with suspiciously active clients.
    AND NOT (user_event_count > 50 AND event_type = 'click')
  GROUP BY
    submission_date,
    source,
    event_type,
    form_factor,
    country,
    subdivision1,
    advertiser,
    release_channel,
    position,
    provider,
    match_type,
    normalized_os,
    suggest_data_sharing_enabled
 ),
 -- event_aggregates_extended
-- sponsored tiles data by client id

-- not included suggest-specific metrics: 
-- match_type 
-- suggest_data_sharing

-- additional not included metrics:
-- advertiser
-- provider (built off of reporting_url which has advertiser info)
-- source (since only topsites for now)
-- position since not in the event 

with newtab_unnested as (select as struct t.client_info.client_id, 
    date(t.submission_timestamp) as submission_date, 
    SPLIT(t.metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    t.normalized_country_code as country,
    t.metadata.geo.subdivision1 as subdivision1,
    t.normalized_channel,
    s.name,
    s.category,
    s.extra
  from `mozdata.firefox_desktop.newtab` t
  cross join unnest(t.events) s),

desktop_events as (
  -- desktop tiles clicks
  select client_id, 
    submission_date, 
    'click' as event_type,
    'desktop' as device, -- same as form_factor in event_aggregates
    normalized_os,
    country,
    subdivision1,
    normalized_channel
  from newtab_unnested
  where 
    name = "click" AND
    category = "topsites" AND
    mozfun.map.get_key(extra, "is_sponsored") = "true"
UNION ALL 
  -- desktop tiles impressions
  select client_id, 
    submission_date, 
    'impression' as event_type,
    'desktop' as device, -- same as form_factor in event_aggregates
    normalized_os,
    country,
    subdivision1,
    normalized_channel
  from newtab_unnested
  where 
    name = "impression" AND
    category = "topsites" AND
    mozfun.map.get_key(extra, "is_sponsored") = "true"
UNION ALL
-- desktop Sponsored Tile Dismissals
  SELECT
    client_id,
    DATE(submission_timestamp) AS submission_date,
    'Sponsored Tiles Dismissals' AS event_type,
    'desktop' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM
    `mozdata.activity_stream.events` 
  WHERE
    event = 'BLOCK'
    AND value like '%spoc%' 
    AND value like '%card_type%'
UNION ALL
-- desktop Sponsored Tiles Disabled
SELECT
  client_id,
    DATE(submission_timestamp) AS submission_date,
    'Sponsored Tiles Disables' AS event_type,
    'desktop' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
FROM
  `mozdata.activity_stream.events` events
WHERE
  (events.event ) = 'PREF_CHANGED' 
  AND (events.source ) = 'SPONSORED_TOP_SITES'
),

-- merge on measures by client
final_desktop_events AS (
SELECT 
  desktop_events.client_id,
  desktop_events.submission_date,
  desktop_events.event_type,
  desktop_events.device,
  desktop_events.normalized_os,
  desktop_events.country,
  desktop_events.subdivision1,
  desktop_events.normalized_channel,
  clients_last_seen.days_since_created_profile -- note this is only recorded for profiles created in the last month
FROM desktop_events 
LEFT JOIN
  `mozdata.firefox_desktop.clients_last_seen_joined` clients_last_seen
ON
  clients_last_seen.client_id = desktop_events.client_id
  AND clients_last_seen.submission_date = '2022-08-01'
  AND clients_last_seen.submission_date = desktop_events.submission_date
ORDER BY clients_last_seen.days_since_created_profile desc
),

ios_events AS(
  -- iOS clicks
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'click' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.firefox_ios.events_unnested`  events
  WHERE event_category like 'top_site%'
  AND event_name = "contile_click" 
  UNION ALL
  -- iOS impressions
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'impression' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.firefox_ios.events_unnested`  events
  WHERE event_category like 'top_site%'
  AND event_name = "contile_impression" 
  UNION ALL
  -- iOS Sponsored Tiles Disables 
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'Sponsored Tiles Disables' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.firefox_ios.events_unnested`  events
  WHERE event_category = 'preferences'
  AND event_name = "changed" 
  AND `mozfun.map.get_key`(event_extra, 'preference') = 'sponsoredTiles'
  AND `mozfun.map.get_key`(event_extra, 'changed_to') = 'false'
),

android_events AS (
  -- Android clicks
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'click' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.fenix.events_unnested`  events
  WHERE event_category = 'top_sites'
  AND event_name = "contile_click" 
  UNION ALL
  -- Android impressions
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'impression' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.fenix.events_unnested`  events
  WHERE event_category = 'top_sites'
  AND event_name = "contile_impression" 
  UNION ALL
  -- Android Sponsored Tiles Disables
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    'Sponsored Tiles Disables' AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM `mozdata.fenix.events_unnested`  
  WHERE event_category = 'customize_home'
    AND event_name = "preference_toggled"
    AND `mozfun.map.get_key`(event_extra, 'preference_key') = 'contile'
    AND `mozfun.map.get_key`(event_extra, 'enabled') = 'false'
  UNION ALL
  -- Android Sponsored Tiles Enabled at Startup
  -- Android Sponsored Tiles Disabled at Startup
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    CASE WHEN metrics.boolean.customize_home_contile THEN 'Sponsored Tiles Enabled at Startup' ELSE 'Sponsored Tiles Disabled at Startup' END AS event_type,
    'mobile' AS device,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    normalized_country_code as country,
    metadata.geo.subdivision1,
    normalized_channel
  FROM  `mozdata.fenix.metrics` 
  WHERE metrics.boolean.customize_home_contile IS NOT NULL
),

-- merge on measures by client
final_ios_events AS (
SELECT 
  ios_events.client_id,
  ios_events.submission_date,
  ios_events.event_type,
  ios_events.device,
  ios_events.normalized_os,
  ios_events.country,
  ios_events.subdivision1,
  ios_events.normalized_channel,
  clients_last_seen.days_since_created_profile -- note this is only recorded for profiles created in the last month
FROM ios_events 
LEFT JOIN
  `mozdata.firefox_ios.clients_last_seen_joined` clients_last_seen
ON
  clients_last_seen.client_id = ios_events.client_id
  AND clients_last_seen.submission_date = ios_events.submission_date
ORDER BY clients_last_seen.days_since_created_profile desc
),

final_android_events AS (
SELECT 
  android_events.client_id,
  android_events.submission_date,
  android_events.event_type,
  android_events.device,
  android_events.normalized_os,
  android_events.country,
  android_events.subdivision1,
  android_events.normalized_channel,
  clients_last_seen.days_since_created_profile -- note this is only recorded for profiles created in the last month
FROM android_events 
LEFT JOIN
  `mozdata.fenix.clients_last_seen_joined` clients_last_seen
ON
  clients_last_seen.client_id = android_events.client_id
  AND clients_last_seen.submission_date = android_events.submission_date
ORDER BY clients_last_seen.days_since_created_profile desc
), 

desktop_and_mobile_events AS (
-- combine desktop and mobile
SELECT *
FROM final_desktop_events
UNION ALL
SELECT *
FROM final_ios_events
UNION ALL 
SELECT *
FROM final_android_events
)
SELECT *
FROM contextual_services_events
UNION ALL
SELECT *
FROM desktop_and_mobile_events

