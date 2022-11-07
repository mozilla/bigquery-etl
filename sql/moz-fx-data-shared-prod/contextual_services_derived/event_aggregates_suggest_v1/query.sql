WITH combined AS (
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    LOWER(advertiser) AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    -- 1-based position
    (position + 1) AS position,
    IF(request_id IS NULL, 'remote settings', 'merino') AS provider,
    match_type,
    coalesce(
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online',
      FALSE
    ) AS suggest_data_sharing_enabled,
    'impression' AS event_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.quicksuggest_impression`
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    LOWER(advertiser) AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    -- 1-based position
    (position + 1) AS position,
    IF(request_id IS NULL, 'remote settings', 'merino') AS provider,
    match_type,
    coalesce(
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online',
      FALSE
    ) AS suggest_data_sharing_enabled,
    'click' AS event_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.quicksuggest_click`
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
GROUP BY
  submission_date,
  form_factor,
  country,
  advertiser,
  normalized_os,
  release_channel,
  position,
  provider,
  match_type,
  suggest_data_sharing_enabled
