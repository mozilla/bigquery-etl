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
  FROM
    contextual_services_stable.quicksuggest_impression_v1
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
  FROM
    contextual_services_stable.quicksuggest_click_v1
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
  FROM
    contextual_services_stable.topsites_impression_v1
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
  FROM
    contextual_services_stable.topsites_click_v1
)
SELECT
  * EXCEPT (context_id),
  COUNT(*) AS event_count,
  COUNT(DISTINCT(context_id)) AS user_count,
FROM
  combined
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  source,
  event_type,
  country,
  subdivision1,
  advertiser,
  release_channel,
  position
ORDER BY
  source,
  event_type
