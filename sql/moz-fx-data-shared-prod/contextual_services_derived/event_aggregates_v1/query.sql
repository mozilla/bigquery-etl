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
)
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
  AND IF(
    DATE_DIFF(CURRENT_DATE(), @submission_date, DAY) > 30,
    ERROR("Data older than 30 days has been removed"),
    NULL
  ) IS NULL
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
