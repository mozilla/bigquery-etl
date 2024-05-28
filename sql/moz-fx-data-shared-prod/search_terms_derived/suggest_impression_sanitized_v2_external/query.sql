/*

This query is run in a standalone project and is scheduled separately from our
normal Airflow infrastructure, but is codified here for discoverability and for
making use of the SQL test harness.

Note that we need to fully qualify all table references since this runs from
a separate project.

The results of this are copied into suggest_impression_sanitized_v2,
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
merino_logs AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, SECOND) AS merino_timestamp,
    jsonPayload.fields.rid AS request_id,
    LTRIM(LOWER(jsonPayload.fields.query)) AS query,
    -- Merino currently injects 'none' for missing geo fields.
    NULLIF(jsonPayload.fields.country, 'none') AS merino_country,
    NULLIF(jsonPayload.fields.region, 'none') AS merino_region,
    NULLIF(jsonPayload.fields.dma, 'none') AS merino_dma,
    -- -- We are not propagating city-level data to sanitized table.
    -- NULLIF(jsonPayload.fields.city, 'none') AS merino_city,
    jsonPayload.fields.form_factor AS merino_form_factor,
    jsonPayload.fields.browser AS merino_browser,
    jsonPayload.fields.os_family AS merino_os_family,
    -- merino_version will be added once implemented in Merino logging code
  FROM
    `suggest-searches-prod-a30f.logs.stdout`
  WHERE
    DATE(timestamp) = @submission_date
    AND jsonPayload.type = "web.suggest.request"
),
allowed_queries AS (
  SELECT
    -- Each keyword should only appear once, but we add DISTINCT for protection
    -- in downstream joins in case the suggestions file has errors.
    DISTINCT query
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.remotesettings_suggestions_v1`
  CROSS JOIN
    UNNEST(keywords) AS query
),
merino_sanitized AS (
  SELECT
    IF(allowed_queries.query IS NOT NULL, query, '<disallowed>') AS sanitized_query,
    merino_logs.* EXCEPT (query)
  FROM
    merino_logs
  LEFT JOIN
    allowed_queries
    USING (query)
)
SELECT
  * EXCEPT (request_id, sanitized_query, telemetry_query),
  COALESCE(sanitized_query, telemetry_query) AS sanitized_query,
FROM
  impressions
LEFT JOIN
  merino_sanitized
  USING (request_id)
