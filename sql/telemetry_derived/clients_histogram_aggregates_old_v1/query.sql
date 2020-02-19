SELECT
  sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  hist_aggs.channel AS channel,
  CONCAT(client_id, os, app_version, app_build_id, hist_aggs.channel) AS join_key,
  histogram_aggregates
FROM clients_histogram_aggregates_v1 AS hist_aggs
LEFT JOIN latest_versions
ON latest_versions.channel = hist_aggs.channel
WHERE app_version >= (latest_version - 2)