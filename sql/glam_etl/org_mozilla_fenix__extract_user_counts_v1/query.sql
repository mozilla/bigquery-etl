-- query for org_mozilla_fenix__extract_user_counts_v1;
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
    `glam_etl.org_mozilla_fenix__view_user_counts_v1`
)
SELECT
  channel,
  app_version,
  coalesce(ping_type, "*") AS ping_type,
  COALESCE(app_build_id, "*") AS app_build_id,
  COALESCE(os, "*") AS os,
  total_users
FROM
  deduped
WHERE
  rank = 1;
