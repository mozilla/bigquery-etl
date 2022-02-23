SELECT
  cp.channel,
  cp.app_version,
  cp.ping_type,
  COALESCE(cp.app_build_id, "*") AS app_build_id,
  COALESCE(cp.os, "*") AS os,
  cp.metric,
  cp.key,
  cp.client_agg_type,
  total_sample
FROM
  `{{ dataset }}.{{ prefix }}__view_probe_counts_v1` cp
LEFT JOIN  `{{ dataset }}.{{ prefix }}__view_sample_counts_v1` sc
ON sc.os=COALESCE(cp.os, "*")
AND sc.app_build_id=COALESCE(cp.app_build_id, "*")
AND sc.app_version=cp.app_version
AND sc.metric=cp.metric
AND cp.key=sc.key
AND total_sample IS NOT NULL 
WHERE
  cp.app_version IS NOT NULL
  AND total_users > {{ total_users }}
  AND cp.client_agg_type not in ('sum','min','avg','max')