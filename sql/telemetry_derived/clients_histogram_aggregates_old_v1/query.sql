SELECT
  client_id,
  os,
  app_version,
  app_build_id,
  histogram_aggs.channel AS channel,
  channel_enum,
  CONCAT(client_id, os, app_version, app_build_id, histogram_aggs.channel) AS join_key,
  histogram_aggregates
FROM clients_histogram_aggregates_v1 AS histogram_aggs
LEFT JOIN latest_versions
ON latest_versions.channel = histogram_aggs.channel
WHERE app_version >= (latest_version - 2)