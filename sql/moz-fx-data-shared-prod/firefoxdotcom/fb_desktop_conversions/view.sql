CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.fb_desktop_conversions`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.fb_desktop_conversions_v1`
WHERE
  -- TODO: given we're pushing the data we probably can just send the most recent activity_date (ds - 2 days)
  activity_date >= DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
  AND ga_country IN ("United States", "India")
