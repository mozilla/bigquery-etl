--- User-facing view. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.active_users_aggregates`
AS
SELECT
  * EXCEPT (app_version, app_name),
  app_name,
  app_version,
  `mozfun.norm.browser_version_info`(app_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(app_version).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(app_version).is_major_release AS app_version_is_major_release,
  `mozfun.norm.os`(os) AS os_grouped
FROM
  `moz-fx-data-shared-prod.fenix_derived.active_users_aggregates_v3`
