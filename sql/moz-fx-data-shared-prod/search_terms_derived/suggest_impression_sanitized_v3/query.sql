/*
This query is here to join Firefox Suggest impression pings
with sanitized search query data captured in logs from the backend Merino service.

The results of this are copied into suggest_impression_sanitized_v3,
which is also defined in this directory.
*/
WITH impressions AS (
  SELECT
    -- This should already be truncated to second level per CONSVC-1364
    -- but we reapply truncation to be explicit about granularity.
    TIMESTAMP_TRUNC(submission_timestamp, SECOND) AS submission_timestamp,
    request_id,
    -- Firefox allows casing and whitespace differences when matching to the
    -- list of suggestions in RemoteSettings.
    LTRIM(LOWER(search_query)) AS telemetry_query,
    advertiser,
    block_id,
    context_id,
    sample_id,
    is_clicked,
    locale,
    metadata.geo.country,
    metadata.geo.subdivision1 AS region,
    normalized_os,
    normalized_os_version,
    release_channel AS normalized_channel,
    position,
    reporting_url,
    scenario,
    -- Truncate to just Firefox major version
    SPLIT(version, '.')[SAFE_OFFSET(0)] AS version,
  FROM
    `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, SECOND) AS submission_timestamp,
    metrics.string.quick_suggest_request_id AS request_id,
    NULL AS telemetry_query,
    metrics.string.quick_suggest_advertiser AS advertiser,
    SAFE_CAST(metrics.string.quick_suggest_block_id AS INT64) AS block_id,
    metrics.uuid.quick_suggest_context_id AS context_id,
    sample_id,
    metrics.boolean.quick_suggest_is_clicked AS is_clicked,
    client_info.locale AS locale,
    metadata.geo.country,
    metadata.geo.subdivision1 AS region,
    normalized_os,
    normalized_os_version,
    normalized_channel,
    metrics.quantity.quick_suggest_position AS position,
    metrics.url2.quick_suggest_reporting_url AS reporting_url,
    NULL AS scenario,
    -- Truncate to just Firefox major version
    SPLIT(client_info.app_display_version, '.')[SAFE_OFFSET(0)] AS version,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.quick_suggest_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.quick_suggest_ping_type = 'quicksuggest-impression'
),
sanitized_queries AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, SECOND) AS timestamp,
    LTRIM(LOWER(query)) AS query,
    * EXCEPT (timestamp, query, region, country)
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`
  WHERE
    DATE(timestamp) = @submission_date
),
sanitized_queries_count AS (
  SELECT
    COUNT(*) AS _n,
    COUNT(query) AS _n_with_query,
  FROM
    sanitized_queries
),
-- We perform a LEFT JOIN on TRUE as a workaround to attach the count to every
-- row from the impressions table; the LEFT JOIN has the important property that
-- if the input impressions partition is empty, we will still get a single row of
-- output, which allows us to raise an error in the WHERE clause.
validated_queries AS (
  SELECT
    * EXCEPT (_n, _n_with_query),
  FROM
    sanitized_queries_count
  LEFT JOIN
    sanitized_queries
    ON TRUE
  WHERE
    IF(
      _n < 1,
      ERROR(
        "The source partition of moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3 is empty; retry later or investigate upstream issues"
      ),
      TRUE
    )
)
SELECT
  *
FROM
  impressions
LEFT JOIN
  validated_queries
  USING (request_id)
