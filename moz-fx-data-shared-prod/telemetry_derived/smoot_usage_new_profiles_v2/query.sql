WITH
  base AS (
  SELECT * FROM smoot_usage_desktop_v2
  UNION ALL
  SELECT * FROM smoot_usage_nondesktop_v2
  UNION ALL
  SELECT * FROM smoot_usage_fxa_v2
  )
  --
SELECT
  submission_date,
  CASE usage
    WHEN 'Any Firefox Account Activity' THEN 'New Firefox Account Registered'
    ELSE REGEXP_REPLACE(usage, 'Any (.*) Activity', 'New \\1 Profile Created')
  END AS usage,
  metrics.day_6.new_profiles,
  * EXCEPT (submission_date, usage, metrics)
FROM
  base
WHERE
  usage LIKE 'Any %'
  AND metrics.day_6.new_profiles > 0
  -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
  AND (@submission_date IS NULL OR @submission_date = submission_date)
