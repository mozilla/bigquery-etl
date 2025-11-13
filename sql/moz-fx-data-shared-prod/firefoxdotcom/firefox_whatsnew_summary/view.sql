CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.firefox_whatsnew_summary`
AS
SELECT
  wns.*,
  map.english_text
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.firefox_whatsnew_summary_v2` wns
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
