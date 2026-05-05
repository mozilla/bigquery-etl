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
  ) AS is_desktop
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v3` AS a
