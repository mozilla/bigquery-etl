MERGE INTO
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v1` AS T
  USING (
    SELECT
      n.client_id,
      n.linked_client_id,
      COALESCE(old.first_seen_linkage_date, n.submission_date) AS first_seen_linkage_date,
      n.submission_date AS last_seen_linkage_date
    FROM
      (
        SELECT
          client_id,
          linked_client_id,
          submission_date
        FROM
          `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_staging_v1`
        WHERE
          submission_date = @submission_date
      ) n
    LEFT JOIN
      `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v1` old
      ON n.client_id = old.client_id
      AND n.linked_client_id = old.linked_client_id
  ) AS S
  ON T.client_id = S.client_id
  AND T.linked_client_id = S.linked_client_id
WHEN NOT MATCHED BY TARGET
THEN
  INSERT
    (client_id, linked_client_id, first_seen_linkage_date, last_seen_linkage_date)
  VALUES
    (S.client_id, S.linked_client_id, S.first_seen_linkage_date, S.last_seen_linkage_date)
  WHEN MATCHED
THEN
  UPDATE
    SET T.last_seen_linkage_date = S.last_seen_linkage_date
