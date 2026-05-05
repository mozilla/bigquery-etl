SELECT
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-shared-prod.pocket_derived.spoc_tile_ids_history_v1`
WHERE
  submission_date = @submission_date
