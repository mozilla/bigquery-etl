CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.ga_sessions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2`
WHERE
  REGEXP_CONTAINS(ga_client_id || '-' || ga_session_id, r"^[0-9]+\.{1}[0-9]+\-{1}[0-9]+$")
