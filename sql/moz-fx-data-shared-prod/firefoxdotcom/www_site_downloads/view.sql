CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.www_site_downloads`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.www_site_downloads_v1`
WHERE
  -- Excluding events from the testing phase.
  submission_date >= "2025-07-16"
