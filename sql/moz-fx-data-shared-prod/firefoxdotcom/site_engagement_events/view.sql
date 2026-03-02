CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.site_engagement_events`
AS
SELECT
  e.ga_client_id || "-" || e.ga_session_id AS visit_identifier,
  e.*,
  s.engaged_session
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.site_engagement_events_v1` e
JOIN
  `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2` s
  ON e.ga_client_id = s.ga_client_id
  AND e.ga_session_id = s.ga_session_id
WHERE
  REGEXP_CONTAINS(s.ga_client_id || '-' || s.ga_session_id, r"^[0-9]+\.[0-9]+\-[0-9]+$")
