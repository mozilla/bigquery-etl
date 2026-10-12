--- User-facing view. Generated via sql_generators.active_users.
--- This view returns Glean data for the full history: https://mozilla-hub.atlassian.net/browse/DENG-970
---
--- TEMPORARY (DENG-11615): active_users_aggregates_v4 only holds partitions from
--- 2026-06-07 onwards, so history is read from active_users_aggregates_v3 until v4
--- has been backfilled. Once that backfill is verified, drop the union and read
--- {{ table_name }} directly.
---
--- UNION ALL BY NAME is required: the two tables declare the same 28 columns in a
--- different order, so a positional UNION ALL would misalign them.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
WITH unioned AS (
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}_derived.{{ table_name }}`
  WHERE
    submission_date >= '2026-06-07'
  UNION ALL BY NAME
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v3`
  WHERE
    submission_date < '2026-06-07'
)
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
  unioned
WHERE
  app_name != 'Focus Android Legacy'
