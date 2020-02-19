WITH clients_histogram_aggregates_partition AS
  (SELECT *
  FROM clients_histogram_aggregates_v1
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id)

SELECT
  sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  hist_aggs.channel AS channel,
  CONCAT(client_id, os, app_version, app_build_id, hist_aggs.channel) AS join_key,
  histogram_aggregates
FROM clients_histogram_aggregates_partition AS hist_aggs
LEFT JOIN latest_versions
ON latest_versions.channel = hist_aggs.channel
WHERE app_version >= (latest_version - 2)