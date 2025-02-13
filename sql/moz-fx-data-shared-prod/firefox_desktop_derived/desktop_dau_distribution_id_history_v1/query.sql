-- The desktop baseline ping has historically not had the `distribution_id` field required for us to be able to filter
-- MozillaOnline clients from the KPI counts. We anticipate that this field will be added to the baseline ping for the
-- launch of 136.
-- This table creates a time series of distribution_ids from legacy (main) pings for desktop DAU based on the baseline
-- ping.
-- This would enable us to filter MozillaOnline (distribution_id='MozillaOnline') clients from the KPI purposes
WITH baseline_users AS (
  SELECT
    submission_date,
    client_id,
    legacy_telemetry_client_id,
    (browser_engagement_uri_count > 0)
    AND (browser_engagement_active_ticks > 0) AS is_dau_baseline,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1`
  WHERE
    submission_date = @submission_date
    AND LOWER(IFNULL(isp, '')) <> "browserstack"
    AND client_id IS NOT NULL
    AND legacy_telemetry_client_id IS NOT NULL
),
legacy_active_users AS (
  SELECT
    submission_date,
    client_id AS legacy_telemetry_client_id,
    is_dau AS is_dau_legacy,
    distribution_id
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
    AND LOWER(IFNULL(isp_name, '')) <> "browserstack"
   -- only clients with non-null distribution_ids, reduces the size of the table.
    AND distribution_id IS NOT NULL
    AND is_daily_user
    AND client_id IS NOT NULL
)
SELECT
  submission_date,
  client_id,
  distribution_id
FROM
  legacy_active_users
JOIN
  baseline_users
  USING (submission_date, legacy_telemetry_client_id)
