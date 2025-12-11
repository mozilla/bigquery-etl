CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.firefox_whatsnew_summary`
AS
SELECT
  wn.ga_client_id || "-" || wn.ga_session_id AS visit_identifier,
  s.engaged_session,
  wn.* EXCEPT (ga_client_id, ga_session_id)
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.firefox_whatsnew_summary_v3` wn
JOIN
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v3` s
  ON wn.ga_client_id = s.ga_client_id
  AND wn.ga_session_id = s.ga_session_id
WHERE
  REGEXP_CONTAINS(s.ga_client_id || '-' || s.ga_session_id, r"^[0-9]+\.{1}[0-9]+\-{1}[0-9]+$")
