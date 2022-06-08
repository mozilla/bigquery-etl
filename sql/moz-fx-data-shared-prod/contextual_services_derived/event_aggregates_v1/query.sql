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
    CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    CASE
    WHEN
      metrics.url2.top_sites_contile_reporting_url IS NULL
    THEN
      'remote settings'
    ELSE
      'contile'
    END
    AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
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
    CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    CASE
    WHEN
      metrics.url2.top_sites_contile_reporting_url IS NULL
    THEN
      'remote settings'
    ELSE
      'contile'
    END
    AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
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
    CAST(
      (SELECT value FROM UNNEST(events[SAFE_OFFSET(0)].extra) WHERE key = 'position') AS INT64
    ) AS position,
    CASE
    WHEN
      metrics.url2.top_sites_contile_reporting_url IS NULL
    THEN
      'remote settings'
    ELSE
      'contile'
    END
    AS provider,
    -- `match_type` is only available for `quicksuggest_*` tables
    NULL AS match_type,
  FROM
    org_mozilla_fenix.topsites_impression
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
  match_type
