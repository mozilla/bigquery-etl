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
),
sanitized_queries AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`
  WHERE
    DATE(timestamp) = @submission_date
)
SELECT
  *
FROM
  sanitized_queries
LEFT JOIN
  impressions
USING
  (request_id)
