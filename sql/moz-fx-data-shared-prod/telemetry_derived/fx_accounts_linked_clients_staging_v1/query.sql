WITH client_id_to_account_stg AS (
  SELECT DISTINCT
    client_info.client_id AS client_id,
    metrics.string.client_association_uid AS account_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE
    client_info.client_id IS NOT NULL
    AND metrics.string.client_association_uid IS NOT NULL
    AND DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 30 day)
)
SELECT DISTINCT
  a.client_id,
  b.client_id AS linked_client_id,
  @submission_date AS submission_date
FROM
  client_id_to_account_stg a
JOIN
  client_id_to_account_stg b
  ON a.account_id = b.account_id
  AND a.client_id < b.client_id
