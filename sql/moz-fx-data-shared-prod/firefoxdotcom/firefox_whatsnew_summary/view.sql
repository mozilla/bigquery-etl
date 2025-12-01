CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.firefox_whatsnew_summary`
AS
SELECT
  wns.ga_client_id || "-" || wns.ga_session_id AS visit_identifier,
  wns.*,
  map.english_text,
  s.engaged_session
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.firefox_whatsnew_summary_v2` wns
JOIN
  `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2` s
  ON wns.ga_client_id = s.ga_client_id
  AND wns.ga_session_id = s.ga_session_id
LEFT JOIN
  (
  --this is an extra safeguard, there should only ever be 1 row per UID & locale
    SELECT
      data_cta_uid,
      locale,
      english_text,
      ROW_NUMBER() OVER (PARTITION BY data_cta_uid, locale ORDER BY RAND() ASC) AS rnk
    FROM
      `moz-fx-data-shared-prod.firefoxdotcom_derived.firefox_data_cta_uid_map_v1`
    QUALIFY
      rnk = 1
  ) map
  ON wns.cta_click_uid = map.data_cta_uid
  AND wns.page_location_locale = map.locale
WHERE
  REGEXP_CONTAINS(s.ga_client_id || '-' || s.ga_session_id, r"^[0-9]+\.{1}[0-9]+\-{1}[0-9]+$")
