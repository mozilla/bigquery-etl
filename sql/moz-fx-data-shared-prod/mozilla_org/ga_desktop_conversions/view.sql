CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.ga_desktop_conversions`
AS
SELECT
  activity_date AS activity_dt,
  activity_datetime AS activity_date,
  gclid,
  conversion_name
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_desktop_conversions_v1`
