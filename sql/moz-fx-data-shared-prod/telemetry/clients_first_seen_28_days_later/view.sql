CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen_28_days_later`
AS
SELECT
  a.*,
  mozdata.udf.funnel_derived_clients(
    channel,
    os,
    first_seen_date,
    build_id,
    attribution_source,
    attribution_ua,
    startup_profile_selection_reason,
    distribution_id
  ) AS funnel_derived,
  mozdata.udf.distribution_model_clients(distribution_id) AS distribution_model
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_28_days_later_v1` a
