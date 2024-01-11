CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stub_attribution_service.dl_token_ga_attribution_lookup`
AS
SELECT
  * REPLACE (
    mozfun.ga.nullify_string(dl_token) AS dl_token,
    mozfun.ga.nullify_string(ga_client_id) AS ga_client_id,
    mozfun.ga.nullify_string(stub_session_id) AS stub_session_id
  )
FROM
  `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
