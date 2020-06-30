{{ header }}
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
        PARTITION BY channel, app_version, ping_type, app_build_id, os
        ORDER BY total_users DESC
    ) AS rank
  FROM
    `{{ dataset }}.{{ prefix }}__view_user_counts_v1`
)

SELECT
  channel,
  app_version,
  coalesce(ping_type, "*") as ping_type,
  COALESCE(CAST({{ build_date_udf }}(app_build_id) AS STRING), "*") AS app_build_id,
  COALESCE(os, "*") AS os,
  total_users
FROM deduped
WHERE rank = 1;
