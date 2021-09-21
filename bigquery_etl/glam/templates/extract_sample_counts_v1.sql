{{ header }}

SELECT
  channel,
  app_version,
  metric, 
  key,
  coalesce(ping_type, "*") as ping_type,
  COALESCE(app_build_id, "*") as app_build_id,
  IF(app_build_id="*", NULL, SAFE_CAST({{ build_date_udf }}(app_build_id) AS STRING))AS build_date,
  COALESCE(os, "*") AS os,
  total_sample
FROM
    `{{ dataset }}.{{ prefix }}__view_sample_counts_v1`