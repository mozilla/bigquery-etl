CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen_28_days_later`
AS
SELECT
  a.*,
  udf.funnel_derived_clients(
    channel,
    os,
    first_seen_date,
    build_id,
    attribution_source,
    attribution_ua,
    startup_profile_selection_reason,
    distribution_id
  ) AS funnel_derived,
  udf.distribution_model_clients(distribution_id) AS distribution_model,
  udf.partner_org_clients(distribution_id) AS partner_org
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_28_days_later_v1` a
