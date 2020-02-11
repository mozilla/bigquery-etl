SELECT
  COALESCE(old_data.client_id, new_data.client_id) AS client_id,
  COALESCE(old_data.os, new_data.os) AS os,
  COALESCE(old_data.app_version, CAST(new_data.app_version AS INT64)) AS app_version,
  COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
  COALESCE(old_data.channel, new_data.channel) AS channel,
  COALESCE(old_data.channel_enum, new_data.channel_enum) AS channel_enum,
  old_data.histogram_aggregates AS old_aggs,
  new_data.histogram_aggregates AS new_aggs
FROM clients_histogram_aggregates_old_v1 AS old_data
FULL OUTER JOIN clients_histogram_aggregates_new_v1 AS new_data
  ON new_data.join_key = old_data.join_key
WHERE old_data.channel_enum = @channel_enum
  AND new_data.channel_enum = @channel_enum