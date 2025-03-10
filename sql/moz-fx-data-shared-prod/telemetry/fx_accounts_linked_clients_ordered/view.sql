CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_accounts_linked_clients_ordered`
AS
WITH first_seen_date_by_glean_client_id AS (
  SELECT
    client_id,
    first_seen_date
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_first_seen`
  WHERE
    submission_date <= CURRENT_DATE
),
fxa_linked_plus_fsd AS (
  SELECT
    a.client_id,
    a.linked_client_id,
    a.linkage_first_seen_date,
    a.linkage_last_seen_date,
    b.first_seen_date AS client_id_first_seen_date,
    c.first_seen_date AS linked_client_id_first_seen_date
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_linked_clients_v1` a
  LEFT JOIN
    first_seen_date_by_glean_client_id b
    ON a.client_id = b.client_id
  LEFT JOIN
    first_seen_date_by_glean_client_id c
    ON a.linked_client_id = c.client_id
)
--re-order so client ID has a first seen date less than or equal to linked client ID
SELECT
  client_id,
  linked_client_id,
  linkage_first_seen_date,
  linkage_last_seen_date,
  client_id_first_seen_date,
  linked_client_id_first_seen_date
FROM
  fxa_linked_plus_fsd
WHERE
  client_id_first_seen_date <= linked_client_id_first_seen_date
UNION ALL
SELECT
  linked_client_id AS client_id,
  client_id AS linked_client_id,
  linkage_first_seen_date,
  linkage_last_seen_date,
  linked_client_id_first_seen_date AS client_id_first_seen_date,
  client_id_first_seen_date AS linked_client_id_first_seen_date
FROM
  fxa_linked_plus_fsd
WHERE
  client_id_first_seen_date > linked_client_id_first_seen_date
