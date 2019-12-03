SELECT
  main_summary_v4.*,
  key AS experiment_id,
  udf.get_key('branch', value) AS experiment_branch,
  udf.get_key('enrollment_id', value) AS experiment_enrollment_id
FROM
  main_summary_v4
CROSS JOIN
  UNNEST(experiments_details)
WHERE
  submission_date = @submission_date
  AND key IN UNNEST(@experiment_list)
  -- skip runs with an empty @experiment_list
  AND ARRAY_LENGTH(@experiment_list) > 0
