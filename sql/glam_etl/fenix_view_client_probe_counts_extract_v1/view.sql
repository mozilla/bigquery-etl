CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.fenix_view_client_probe_counts_extract_v1`
AS
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
    `moz-fx-data-shared-prod.glam_etl.fenix_view_client_probe_counts_v1`
  WHERE
    app_version IS NOT NULL
    AND total_users > 1000
)
SELECT
  channel,
  app_version,
  agg_type,
  COALESCE(ping_type, "*") AS ping_type,
  COALESCE(os, "*") AS os,
  COALESCE(app_build_id, "*") AS app_build_id,
  metric,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
  SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS key,
  client_agg_type,
  metric_type,
  total_users,
  TO_JSON_STRING(aggregates) AS aggregates
FROM
  deduped
WHERE
  rank = 1
