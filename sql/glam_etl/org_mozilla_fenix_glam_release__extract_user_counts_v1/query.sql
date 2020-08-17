-- query for org_mozilla_fenix_glam_release__extract_user_counts_v1;
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        channel,
        app_version,
        ping_type,
        app_build_id,
        os
      ORDER BY
        total_users DESC
    ) AS rank
  FROM
    `glam_etl.org_mozilla_fenix_glam_release__view_user_counts_v1`
)
SELECT
  channel,
  app_version,
  coalesce(ping_type, "*") AS ping_type,
  COALESCE(app_build_id, "*") AS app_build_id,
  SAFE_CAST(
    `moz-fx-data-shared-prod`.udf.fenix_build_to_datetime(app_build_id) AS STRING
  ) AS build_date,
  COALESCE(os, "*") AS os,
  total_users
FROM
  deduped
WHERE
  rank = 1;
