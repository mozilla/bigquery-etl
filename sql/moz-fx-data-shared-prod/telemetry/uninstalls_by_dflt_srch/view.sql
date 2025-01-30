CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_by_dflt_srch`
AS
SELECT
  submission_date,
  default_search_engine,
  nbr_unique_clients_uninstalling AS uninstalls_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_by_default_search_engine`
