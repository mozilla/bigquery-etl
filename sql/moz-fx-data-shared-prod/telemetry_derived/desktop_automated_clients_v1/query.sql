-- Source of truth for confirmed automated (bot) Firefox Desktop clients. Each confirmed fleet
-- is one rule below, and its value goes into the `fleet` column. The fleet list (value, ticket,
-- onset, rule summary) is in this table's metadata.yaml.
--
-- One row per flagged legacy client_id per submission_date. A client can appear on more than
-- one day; use telemetry.desktop_automated_clients for one row per client. Downstream tables
-- exclude bots by joining to this table, so that a new fleet needs no change to their code.
--
-- To add a fleet:
--   1. Data Science confirms the rule in a Jira ticket.
--   2. Add a signal to client_signals (legacy pings) or glean_signals (Glean baseline ping), and
--      a WHEN branch to the fleet CASE in flagged_clients.
--      Give the rule a comment block with the fleet value, ticket, onset date, approver and a
--      short description.
--   3. Add the fleet to the list in metadata.yaml.
--   4. Backfill this table from the fleet's onset date.
--   5. Backfill the downstream tables that are materialized (not views) for the same dates.
--      Their code does not change, but their old partitions were computed before the new
--      fleet was in this table. Views pick up the change at once.
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
    -- Fleet `aws` | DENG-11925 | onset 2026-11-04 | approved by Desktop DS ("Linux Bot Wave,
    -- November 2026", Appendix D). Used together with has_argument_profile.
    -- Identical 1 CPU EC2 instances, all US, en-US. 87% of these clients never send a
    -- new-profile ping, so the rule also reads main pings.
    LOGICAL_OR(
      os_version LIKE '%-aws'
      AND cpu_count = 1
      AND isp_name = 'Amazon.com'
      AND country = 'US'
      AND locale = 'en-US'
    ) AS has_aws_environment,
    -- Fleet `bulk` | DENG-11925 | onset 2026-10-28 (variant 2), 2026-11-04 (main variant) |
    -- approved by Desktop DS ("Linux Bot Wave, November 2026", Appendix D, and Jira comment
    -- 1912486).
    -- Both image variants: a new-profile ping launched with `-profile` and a
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
-- Glean baseline pings, grouped by legacy client ID. They give the Glean client IDs for every
-- flagged client, so Glean DAU/MAU can exclude them too, and they carry the signals for fleets
-- that send few or no legacy pings.
glean_signals AS (
  SELECT
    metrics.uuid.legacy_telemetry_client_id AS client_id,
    ARRAY_AGG(DISTINCT client_info.client_id IGNORE NULLS) AS glean_client_ids,
    LOGICAL_OR(
      metrics.string.startup_profile_selection_reason = 'argument-profile'
    ) AS has_glean_argument_profile,
    -- Fleet `hetzner` | DENG-11925 | onset 2026-09-27 | approved by Desktop DS (Jira comment
    -- 1917038). Used together with has_glean_argument_profile.
    -- Official Mozilla .deb builds on Hetzner Online servers, en-US, kernel 6.8. Almost none of
    -- these clients send legacy pings, so the rule reads the Glean baseline ping.
    LOGICAL_OR(
      STARTS_WITH(metadata.isp.name, 'Hetzner Online')
      AND metrics.string.usage_distribution_id = 'mozilla-deb'
      AND COALESCE(client_info.locale, metrics.string.glean_baseline_locale) = 'en-US'
      AND normalized_os_version = '6.8'
    ) AS has_hetzner_environment,
    ARRAY_AGG(
      client_info.app_display_version IGNORE NULLS
      ORDER BY
        client_info.app_display_version DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS app_version,
    ARRAY_AGG(normalized_os_version IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS os_version,
    ARRAY_AGG(metadata.isp.name IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS isp_name,
    ARRAY_AGG(metadata.geo.country IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS country,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'Linux'
    AND metrics.uuid.legacy_telemetry_client_id IS NOT NULL
  GROUP BY
    client_id
),
flagged_clients AS (
  SELECT
    client_id,
    -- When a client matches more than one rule, the first matching WHEN branch sets the fleet.
    -- Add new fleets at the end, so that the fleet of already-flagged clients does not change.
    CASE
      WHEN legacy.has_argument_profile
        AND legacy.has_aws_environment
        THEN 'aws'
      WHEN legacy.has_bulk_signature
        THEN 'bulk'
      WHEN glean.has_glean_argument_profile
        AND glean.has_hetzner_environment
        THEN 'hetzner'
    END AS fleet,
    glean.glean_client_ids,
    COALESCE(legacy.app_version, glean.app_version) AS app_version,
    COALESCE(legacy.os_version, glean.os_version) AS os_version,
    COALESCE(legacy.isp_name, glean.isp_name) AS isp_name,
    COALESCE(legacy.country, glean.country) AS country,
  FROM
    client_signals AS legacy
  FULL OUTER JOIN
    glean_signals AS glean
    USING (client_id)
)
SELECT
  @submission_date AS submission_date,
  client_id,
  glean_client_ids,
  fleet,
  app_version,
  os_version,
  isp_name,
  country,
FROM
  flagged_clients
WHERE
  fleet IS NOT NULL
