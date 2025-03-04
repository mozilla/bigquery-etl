MERGE INTO
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v1` AS T
  USING (
    SELECT
      n.client_id,
      n.linked_client_id,
      COALESCE(old.linkage_first_seen_date, n.submission_date) AS linkage_first_seen_date,
      GREATEST(old.linkage_last_seen_date, n.submission_date) AS linkage_last_seen_date
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
    (client_id, linked_client_id, linkage_first_seen_date, linkage_last_seen_date)
  VALUES
    (S.client_id, S.linked_client_id, S.linkage_first_seen_date, S.linkage_last_seen_date)
  WHEN MATCHED
THEN
  UPDATE
    SET T.linkage_last_seen_date = S.linkage_last_seen_date
