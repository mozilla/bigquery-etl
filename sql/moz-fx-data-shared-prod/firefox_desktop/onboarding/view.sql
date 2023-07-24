CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.onboarding`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.onboarding_v2`
