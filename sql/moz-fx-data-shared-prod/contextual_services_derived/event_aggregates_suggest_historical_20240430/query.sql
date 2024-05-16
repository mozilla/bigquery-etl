{% if is_init() %}
  WITH blocks AS (
    SELECT
      b.id,
      b.queryType AS query_type,
    FROM
      `moz-fx-ads-prod.adm.blocks` b
    QUALIFY
      1 = ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY b.date DESC)
  ),
  mobile_suggest AS (
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
  mobile_suggest_with_event_count AS (
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
    *
  FROM
    `moz-fx-data-shared-prod.contextual_services_derived.event_aggregates_suggest_v1`
  WHERE
    submission_date <= '2024-04-30'
  SELECT
    * EXCEPT (context_id, user_event_count, event_type, query_type),
    COUNTIF(event_type = "impression") AS impression_count,
    COUNTIF(event_type = "click") AS click_count,
    query_type,
  FROM
    with_event_count
  WHERE
    submission_date <= '2024-04-30'
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
    suggest_data_sharing_enabled,
    query_type
{% endif %}
