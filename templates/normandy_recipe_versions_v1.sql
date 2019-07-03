SELECT
  @submission_date as submission_date,
  f.key AS recipe_id,
  f.value AS recipe_revision_id,
  COUNT(f.value) AS count
FROM
  main_summary_v4 m,
  m.scalar_parent_normandy_recipe_freshness.key_value f
WHERE
  submission_date_s3 = @submission_date AND
  sample_id = '6' AND
  f IS NOT NULL AND
  f.value IS NOT NULL
GROUP BY
  f.key,
  f.value
