CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen`
AS
SELECT
  a.*,
  udf.funnel_derived_clients(
    normalized_channel,
    normalized_os,
    first_seen_date,
    app_build_id,
    attribution_source,
    attribution_ua,
    startup_profile_selection_reason,
    distribution_id
  ) AS funnel_derived,
  udf.distribution_model_clients(distribution_id) AS distribution_model,
  udf.partner_org_clients(distribution_id) AS partner_org
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2` a
