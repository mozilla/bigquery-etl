SELECT
  mozfun.norm.fenix_app_info('org_mozilla_fenix', client_info.app_build).*
FROM
  org_mozilla_fenix.metrics
WHERE
  submission_date = '2020-08-01'
LIMIT
  1
-- app_name = Fenix, channel = nightly, app_id = org.mozilla.fenix
