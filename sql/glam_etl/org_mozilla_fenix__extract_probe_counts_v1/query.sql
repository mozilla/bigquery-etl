-- TODO: Remove deduping when dupes are fixed.
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        ping_type,
        os,
        app_version,
        app_build_id,
        channel,
        metric,
        metric_type,
        key,
        client_agg_type,
        agg_type
      ORDER BY
        total_users DESC
    ) AS rank
  FROM
    `glam_etl.org_mozilla_fenix_view_probe_counts_v1`
  WHERE
    channel IS NOT NULL
    AND app_version IS NOT NULL
    AND total_users >= 100
)
SELECT
  channel,
  app_version AS version,
  COALESCE(ping_type, "*") AS ping_type,
  COALESCE(os, "*") AS os,
  COALESCE(app_build_id, "*") AS build_id,
  metric,
  metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
  SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
  client_agg_type,
  agg_type,
  total_users,
  TO_JSON_STRING(aggregates) AS data
FROM
  deduped
WHERE
  rank = 1
