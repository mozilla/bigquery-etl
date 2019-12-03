SELECT
  main_summary_v4.*,
  key AS experiment_id,
  value AS experiment_branch
FROM
  main_summary_v4
CROSS JOIN
  UNNEST(experiments)
WHERE
  submission_date = @submission_date
  AND key IN UNNEST(@experiment_list)
  -- skip runs with an empty @experiment_list
  AND ARRAY_LENGTH(@experiment_list) > 0
