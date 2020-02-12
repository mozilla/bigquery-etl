CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.client_probe_counts_release`
AS
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
        PARTITION BY
            channel, app_version, agg_type, os, app_build_id, process, metric, key, client_agg_type, metric_type
        ORDER BY
            total_users DESC
    ) AS rank
  FROM
    `moz-fx-data-shared-prod.telemetry.client_probe_counts`
  WHERE
    channel="release" )

SELECT
  * EXCEPT(rank)
FROM
  deduped
WHERE
  rank = 1
