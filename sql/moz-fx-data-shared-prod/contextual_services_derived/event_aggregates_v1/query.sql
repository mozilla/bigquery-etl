WITH combined AS (
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'suggest' AS source,
    'impression' AS event_type,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    'unknown' AS provider,
  FROM
    contextual_services.quicksuggest_impression
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'suggest' AS source,
    'click' AS event_type,
    normalized_country_code AS country,
    metadata.geo.subdivision1 AS subdivision1,
    advertiser,
    release_channel,
    position,
    'unknown' AS provider,
  FROM
    contextual_services.quicksuggest_click
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    'impression' AS event_type,
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
  FROM
    contextual_services.topsites_impression
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'topsites' AS source,
    'click' AS event_type,
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
  FROM
    contextual_services.topsites_click
),
with_event_count AS (
  SELECT
    *,
    COUNT(*) OVER (
      PARTITION BY
        submission_date,
        context_id,
        source,
        event_type
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
  AND NOT (user_event_count > 50 AND event_type = 'click')
GROUP BY
  submission_date,
  source,
  event_type,
  country,
  subdivision1,
  advertiser,
  release_channel,
  position,
  provider
