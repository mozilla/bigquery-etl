-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.bounce_tracking_protection`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.bounce_tracking_protection_v1`