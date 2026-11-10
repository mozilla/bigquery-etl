CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata.table_effective_access`
AS
SELECT
  t.source_project,
  t.source_dataset,
  t.source_table,
  CONCAT(t.source_project, '.', t.source_dataset, '.', t.source_table) AS qualified_name,
  t.table_type,
  -- Readable by the mozilla-confidential data-viewers workgroup, whether granted
  -- on the dataset or directly on the table.
  IFNULL(d.mozilla_confidential, FALSE)
  OR EXISTS (
    SELECT
      1
    FROM
      UNNEST(t.workgroups) AS w
    WHERE
      w.name = 'mozilla-confidential/data-viewers'
  ) AS mozilla_confidential,
  ARRAY(SELECT w.name FROM UNNEST(d.workgroups) AS w ORDER BY w.name) AS dataset_workgroups,
  ARRAY(SELECT w.name FROM UNNEST(t.workgroups) AS w ORDER BY w.name) AS table_workgroups,
  -- Effective access is the union of dataset- and table-level grants. The two
  -- don't overlap, since table_workgroup_access already excludes groups its
  -- dataset grants. IFNULL because the upstream views' ARRAY_AGG returns NULL
  -- rather than an empty array when there are no workgroups.
  ARRAY_CONCAT(IFNULL(d.google_groups, []), IFNULL(t.google_groups, [])) AS google_groups,
  ARRAY(
    SELECT AS STRUCT
      w.*
    FROM
      UNNEST(ARRAY_CONCAT(IFNULL(d.workgroups, []), IFNULL(t.workgroups, []))) AS w
    ORDER BY
      w.name
  ) AS workgroups,
  t.collected_at,
FROM
  `moz-fx-data-shared-prod.data_governance_metadata.table_workgroup_access` AS t
LEFT JOIN
  `moz-fx-data-shared-prod.data_governance_metadata.dataset_workgroup_access` AS d
  USING (source_project, source_dataset)
