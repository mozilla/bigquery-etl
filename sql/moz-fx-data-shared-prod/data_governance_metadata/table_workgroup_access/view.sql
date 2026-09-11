CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata.table_workgroup_access`
AS
SELECT
  acc.source_project,
  acc.source_dataset,
  acc.source_table,
  acc.table_type,
  ANY_VALUE(acc.google_groups) AS google_groups,
  -- LEFT JOIN UNNEST plus IGNORE NULLS so that tables with no table-specific
  -- access (the majority) keep their row with an empty array. wg_name rather
  -- than wgs.workgroup as the name, so a workgroup missing from
  -- mozcloud.workgroups still reports its name instead of joining to NULL.
  ARRAY_AGG(
    IF(
      wg_name IS NULL,
      NULL,
      STRUCT(
        wg_name AS name,
        wgs.sponsor AS sponsor,
        wgs.managers AS managers,
        wgs.owners AS owners
      )
    ) IGNORE NULLS
    ORDER BY
      wg_name
  ) AS workgroups,
  acc.collected_at,
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.table_workgroup_access_v1` AS acc
LEFT JOIN
  UNNEST(acc.workgroups) AS wg_name
LEFT JOIN
  `moz-fx-data-shared-prod.mozcloud.workgroups` AS wgs
  ON wg_name = wgs.workgroup
WHERE
  acc.collected_at = (
    SELECT
      MAX(collected_at)
    FROM
      `moz-fx-data-shared-prod.data_governance_metadata_derived.table_workgroup_access_v1`
  )
GROUP BY
  acc.source_project,
  acc.source_dataset,
  acc.source_table,
  acc.table_type,
  acc.collected_at
