WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        os,
        app_version,
        app_build_id,
        channel
      ORDER BY
        total_users DESC
    ) AS rank
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.glam_user_counts_v1`
)
SELECT
  CASE
  WHEN
    channel = "nightly"
  THEN
    1
  WHEN
    channel = "beta"
  THEN
    2
  WHEN
    channel = "release"
  THEN
    3
  END
  AS channel,
  app_version,
  COALESCE(app_build_id, "*") AS app_build_id,
  COALESCE(os, "*") AS os,
  total_users
FROM
  deduped
WHERE
  rank = 1;
