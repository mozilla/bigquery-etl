WITH blocks AS (
  SELECT
    b.id,
    b.queryType AS query_type,
  FROM
    `moz-fx-ads-prod.adm.blocks` b
  WHERE
    b.date <= @submission_date
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY b.date DESC)
),
combined AS (
  SELECT
    metrics.uuid.quick_suggest_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    LOWER(metrics.string.quick_suggest_advertiser) AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    client_info.app_channel AS release_channel,
    metrics.quantity.quick_suggest_position AS position,
    IF(
      NULLIF(metrics.string.quick_suggest_request_id, "") IS NULL,
      'remote settings',
      'merino'
    ) AS provider,
    metrics.string.quick_suggest_match_type AS match_type,
    COALESCE(
      metrics.boolean.quick_suggest_improve_suggest_experience,
      FALSE
    ) AS suggest_data_sharing_enabled,
    IF(
      metrics.string.quick_suggest_ping_type = "quicksuggest-click",
      "click",
      "impression"
    ) AS event_type,
    blocks.query_type,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.quick_suggest` qs
  LEFT JOIN
    blocks
    ON SAFE_CAST(qs.metrics.string.quick_suggest_block_id AS INT) = blocks.id
  WHERE
    metrics.string.quick_suggest_ping_type IN ("quicksuggest-click", "quicksuggest-impression")
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    LOWER(advertiser) AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    position,
    IF(request_id IS NULL, 'remote settings', 'merino') AS provider,
    match_type,
    COALESCE(
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online',
      FALSE
    ) AS suggest_data_sharing_enabled,
    'impression' AS event_type,
    CAST(NULL AS STRING) AS query_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.quicksuggest_impression`
  WHERE
    -- For firefox 116+ use firefox_desktop.quick_suggest instead
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1836283
    SAFE_CAST(metadata.user_agent.version AS INT64) < 116
  UNION ALL
  SELECT
    context_id,
    DATE(submission_timestamp) AS submission_date,
    'desktop' AS form_factor,
    normalized_country_code AS country,
    LOWER(advertiser) AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    release_channel,
    position,
    IF(request_id IS NULL, 'remote settings', 'merino') AS provider,
    match_type,
    COALESCE(
      -- The first check is for Fx 103+, the last two checks are for Fx 102 and prior.
      improve_suggest_experience_checked
      OR request_id IS NOT NULL
      OR scenario = 'online',
      FALSE
    ) AS suggest_data_sharing_enabled,
    'click' AS event_type,
    CAST(NULL AS STRING) AS query_type,
  FROM
    `moz-fx-data-shared-prod.contextual_services.quicksuggest_click`
  WHERE
    -- For firefox 116+ use firefox_desktop.quick_suggest instead
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1836283
    SAFE_CAST(metadata.user_agent.version AS INT64) < 116
  UNION ALL
  -- Suggest Android
  SELECT
    metrics.uuid.fx_suggest_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'phone' AS form_factor,
    normalized_country_code AS country,
    metrics.string.fx_suggest_advertiser AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    client_info.app_channel AS release_channel,
    metrics.quantity.fx_suggest_position AS position,
    -- Only remote settings is in use on mobile
    'remote settings' AS provider,
    -- Only standard suggestions are in use on mobile
    'firefox-suggest' AS match_type,
    -- This is the opt-in for Merino, not in use on mobile
    CAST(NULL AS BOOLEAN) AS suggest_data_sharing_enabled,
    IF(
      metrics.string.fx_suggest_ping_type = "fxsuggest-click",
      "click",
      "impression"
    ) AS event_type,
    blocks.query_type,
  FROM
    `moz-fx-data-shared-prod.fenix.fx_suggest` fs
  LEFT JOIN
    blocks
    ON fs.metrics.quantity.fx_suggest_block_id = blocks.id
  WHERE
    metrics.string.fx_suggest_ping_type IN ("fxsuggest-click", "fxsuggest-impression")
  UNION ALL
  -- Suggest iOS
  SELECT
    metrics.uuid.fx_suggest_context_id AS context_id,
    DATE(submission_timestamp) AS submission_date,
    'phone' AS form_factor,
    normalized_country_code AS country,
    metrics.string.fx_suggest_advertiser AS advertiser,
    SPLIT(metadata.user_agent.os, ' ')[SAFE_OFFSET(0)] AS normalized_os,
    client_info.app_channel AS release_channel,
    metrics.quantity.fx_suggest_position AS position,
    -- Only remote settings is in use on mobile
    'remote settings' AS provider,
    -- Only standard suggestions are in use on mobile
    'firefox-suggest' AS match_type,
    -- This is the opt-in for Merino, not in use on mobile
    CAST(NULL AS BOOLEAN) AS suggest_data_sharing_enabled,
    IF(
      metrics.string.fx_suggest_ping_type = "fxsuggest-click",
      "click",
      "impression"
    ) AS event_type,
    blocks.query_type,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.fx_suggest` fs
  LEFT JOIN
    blocks
    ON fs.metrics.quantity.fx_suggest_block_id = blocks.id
  WHERE
    metrics.string.fx_suggest_ping_type IN ("fxsuggest-click", "fxsuggest-impression")
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
  * EXCEPT (context_id, user_event_count, event_type, query_type),
  COUNTIF(event_type = "impression") AS impression_count,
  COUNTIF(event_type = "click") AS click_count,
  query_type,
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
  normalized_os,
  release_channel,
  position,
  provider,
  match_type,
  suggest_data_sharing_enabled,
  query_type
