SELECT
  * EXCEPT (submission_date)
FROM
  spoc_tile_ids_history_v1
WHERE
  submission_date = @submission_date
