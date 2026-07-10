-- macro checks

#fail
{{ not_null(["weight", "position", "tile_format"], "snapshot_date = @snapshot_date") }}

#fail
{{ min_row_count(1, "snapshot_date = @snapshot_date") }}
-- The global (country IS NULL) propensity set must be present for the partition;
-- downstream consumers fall back to it for countries without their own weights.

#fail
SELECT
  IF(
    COUNTIF(country IS NULL) = 0,
    ERROR("No global (country IS NULL) propensity rows for this snapshot_date"),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  snapshot_date = @snapshot_date;
