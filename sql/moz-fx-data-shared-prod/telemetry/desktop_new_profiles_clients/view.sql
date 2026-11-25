CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_new_profiles_clients`
AS
SELECT
  a.*,
  `moz-fx-data-shared-prod`.udf.funnel_derived_clients(
    normalized_os,
    first_seen_date,
    app_build_id,
    attribution_source,
    attribution_ua,
    startup_profile_selection_reason,
    distribution_id
  ) AS funnel_derived,
  `moz-fx-data-shared-prod`.udf.distribution_model_clients(distribution_id) AS distribution_model,
  `moz-fx-data-shared-prod`.udf.partner_org_clients(distribution_id) AS partner_org,
  IF(
    LOWER(IFNULL(isp_name, '')) <> "browserstack"
    AND LOWER(IFNULL(distribution_id, '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  -- Flags confirmed automated (bot) clients from telemetry.desktop_automated_clients
  -- (DENG-11925): every fleet, including the DENG-11590 segment that this column first flagged.
  -- The column name is kept for compatibility. Exposed as a flag rather than filtered out here
  -- so consumers can choose; dashboard-feeding tables exclude these clients at the source.
  COALESCE(
    a.client_id IN (
      SELECT
        client_id
      FROM
        `moz-fx-data-shared-prod.telemetry.desktop_automated_clients`
    ),
    FALSE
  ) AS is_desktop_argument_profile_automation
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3` AS a
