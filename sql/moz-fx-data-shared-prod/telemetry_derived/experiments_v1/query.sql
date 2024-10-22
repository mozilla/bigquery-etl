SELECT
  main_summary.* EXCEPT (submission_date_s3),
  key AS experiment_id,
  `moz-fx-data-shared-prod.udf.get_key`(value, 'branch') AS experiment_branch,
  `moz-fx-data-shared-prod.udf.get_key`(value, 'enrollment_id') AS experiment_enrollment_id
FROM
  `moz-fx-data-shared-prod.telemetry.main_summary`
CROSS JOIN
  UNNEST(experiments_details)
WHERE
  submission_date = @submission_date
  AND key IN UNNEST(@experiment_list)
  -- skip runs with an empty @experiment_list
  AND ARRAY_LENGTH(@experiment_list) > 0
