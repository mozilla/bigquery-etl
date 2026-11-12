-- Flags Firefox Desktop clients from the automated Linux fleets tracked in DENG-11925.
--
-- One row per flagged legacy client_id per submission_date. A client can appear on more than
-- one day; use telemetry.desktop_automated_clients for one row per client.
--
-- The rules come from the Desktop Data Science analysis "Linux Bot Wave, November 2026". They
-- do not use a Firefox version or a start date, so they do not go stale the way the DENG-11590
-- predicate (`moz-fx-data-shared-prod.udf.is_desktop_argument_profile_automation`) does.
--
-- The fields the rules need (kernel release, CPU count, profile count, WebRender status, GPU
-- adapter, screen size, system locales) are not in telemetry_derived.clients_first_seen_v3, so
-- the rules read the raw pings.
WITH legacy_pings AS (
  SELECT
    client_id,
    application.version AS app_version,
    environment.system.os.version AS os_version,
    environment.system.cpu.count AS cpu_count,
    metadata.isp.name AS isp_name,
    metadata.geo.country AS country,
    environment.settings.locale AS locale,
    payload.processes.parent.scalars.startup_profile_selection_reason,
    CAST(NULL AS INT64) AS startup_profile_count,
    environment.system.gfx.features.webrender.status AS webrender_status,
    environment.system.gfx.adapters[SAFE_OFFSET(0)].description AS gfx_adapter_description,
    environment.settings.is_default_browser,
    ARRAY_LENGTH(environment.settings.intl.system_locales) AS system_locales_count,
    environment.system.gfx.monitors[SAFE_OFFSET(0)].screen_width,
    environment.system.gfx.monitors[SAFE_OFFSET(0)].screen_height,
    FALSE AS is_new_profile_ping,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v5`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'Linux'
  UNION ALL
  SELECT
    client_id,
    application.version AS app_version,
    environment.system.os.version AS os_version,
    environment.system.cpu.count AS cpu_count,
    metadata.isp.name AS isp_name,
    metadata.geo.country AS country,
    environment.settings.locale AS locale,
    payload.processes.parent.scalars.startup_profile_selection_reason,
    payload.processes.parent.scalars.startup_profile_count,
    environment.system.gfx.features.webrender.status AS webrender_status,
    environment.system.gfx.adapters[SAFE_OFFSET(0)].description AS gfx_adapter_description,
    environment.settings.is_default_browser,
    ARRAY_LENGTH(environment.settings.intl.system_locales) AS system_locales_count,
    environment.system.gfx.monitors[SAFE_OFFSET(0)].screen_width,
    environment.system.gfx.monitors[SAFE_OFFSET(0)].screen_height,
    TRUE AS is_new_profile_ping,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.new_profile_v4`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'Linux'
),
client_signals AS (
  SELECT
    client_id,
    -- startup_profile_selection_reason is set only on the first session's pings, so it is
    -- evaluated across all of the client's pings for the day.
    LOGICAL_OR(startup_profile_selection_reason = 'argument-profile') AS has_argument_profile,
    -- AWS fleet: identical 1 CPU EC2 instances, all US, en-US. 87% of these clients never send
    -- a new-profile ping, so the rule also reads main pings.
    LOGICAL_OR(
      os_version LIKE '%-aws'
      AND cpu_count = 1
      AND isp_name = 'Amazon.com'
      AND country = 'US'
      AND locale = 'en-US'
    ) AS has_aws_environment,
    -- Bulk fleet (both image variants): a new-profile ping launched with `-profile` and a
    -- profile count of 0 (which normal profiles never report), plus either:
    --   * the WebRender start-up failure SoftwareRasterizer_FIRST (only 1-10 normal Linux
    --     profiles a day), or
    --   * the environment from the DS rule in DENG-11925 (comment 1912486): llvmpipe software
    --     GPU, not the default browser, no system locales, and a 1920x1080 screen. This
    --     condition catches image variant 2, which does not report the WebRender failure.
    -- Each condition catches profiles that the other misses, so the rule uses both.
    LOGICAL_OR(
      is_new_profile_ping
      AND startup_profile_selection_reason = 'argument-profile'
      AND startup_profile_count = 0
      AND (
        webrender_status LIKE '%SoftwareRasterizer_FIRST%'
        OR (
          STARTS_WITH(gfx_adapter_description, 'llvmpipe')
          AND NOT IFNULL(is_default_browser, FALSE)
          AND IFNULL(system_locales_count, 0) = 0
          AND screen_width = 1920
          AND screen_height = 1080
        )
      )
    ) AS has_bulk_signature,
    ARRAY_AGG(app_version IGNORE NULLS ORDER BY app_version DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS app_version,
    ARRAY_AGG(os_version IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS os_version,
    ARRAY_AGG(isp_name IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS isp_name,
    ARRAY_AGG(country IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS country,
  FROM
    legacy_pings
  WHERE
    client_id IS NOT NULL
  GROUP BY
    client_id
),
flagged_clients AS (
  SELECT
    client_id,
    CASE
      WHEN has_argument_profile
        AND has_aws_environment
        THEN 'aws'
      WHEN has_bulk_signature
        THEN 'bulk'
    END AS fleet,
    app_version,
    os_version,
    isp_name,
    country,
  FROM
    client_signals
  WHERE
    (has_argument_profile AND has_aws_environment)
    OR has_bulk_signature
),
-- Glean client IDs for the flagged clients, so Glean DAU/MAU can exclude them too.
glean_clients AS (
  SELECT
    metrics.uuid.legacy_telemetry_client_id AS client_id,
    ARRAY_AGG(DISTINCT client_info.client_id IGNORE NULLS) AS glean_client_ids,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'Linux'
    AND metrics.uuid.legacy_telemetry_client_id IS NOT NULL
  GROUP BY
    client_id
)
SELECT
  @submission_date AS submission_date,
  flagged_clients.client_id,
  glean_clients.glean_client_ids,
  flagged_clients.fleet,
  flagged_clients.app_version,
  flagged_clients.os_version,
  flagged_clients.isp_name,
  flagged_clients.country,
FROM
  flagged_clients
LEFT JOIN
  glean_clients
  USING (client_id)
