SELECT
  *
FROM
  experiment_search_aggregates_base
WHERE
  date(`timestamp`) = @submission_date
  AND dataset_id = 'telemetry_stable'
