CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen`
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
  -- Flags the automated segment tracked in DENG-11590. Exposed as a flag rather than filtered
  -- out here so consumers can choose; dashboard-feeding tables exclude it at the source.
  `moz-fx-data-shared-prod`.udf.is_desktop_argument_profile_automation(
    normalized_os,
    app_version,
    startup_profile_selection_reason,
    first_seen_date
  ) AS is_desktop_argument_profile_automation
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3` a
