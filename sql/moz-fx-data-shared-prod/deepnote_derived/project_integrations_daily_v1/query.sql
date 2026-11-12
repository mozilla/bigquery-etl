SELECT
  snapshot.snapshot_date,
  snapshot.workspace_id,
  integration.integration_id,
  integration.name AS integration_name,
  integration.type AS integration_type,
  snapshot.project_id,
  snapshot.project_name,
FROM
  `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1` AS snapshot
CROSS JOIN
  UNNEST(snapshot.integrations) AS integration
WHERE
  snapshot.snapshot_date = @submission_date
