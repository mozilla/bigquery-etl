CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.taskclusteretl.person_mozilla_com`
AS
WITH RECURSIVE management_path AS (
  -- Base case
  SELECT
    manager_email,
    email,
    [manager_email] AS management_chain,
    [manager_email, email] AS visited_chain
  FROM
    `moz-fx-data-bq-people.composer_workday.worker`
  WHERE
    currently_active = TRUE
  UNION ALL
  -- Recursive step
  SELECT
    er.manager_email,
    mp.email,
    ARRAY_CONCAT([er.manager_email], mp.management_chain) AS management_chain,
    ARRAY_CONCAT(mp.visited_chain, [er.manager_email]) AS visited_chain
  FROM
    `moz-fx-data-bq-people.composer_workday.worker` AS er
  JOIN
    management_path AS mp
    ON er.email = mp.manager_email
  WHERE
    er.manager_email NOT IN UNNEST(mp.visited_chain)
    AND ARRAY_LENGTH(mp.management_chain) < 50
),
deduped_chain AS (
  SELECT
    email,
    ARRAY_REVERSE(management_chain) AS management_chain
  FROM
    (
      SELECT
        email,
        management_chain,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY ARRAY_LENGTH(management_chain) DESC) AS rn
      FROM
        management_path
    )
  WHERE
    rn = 1
),
manager_names AS (
  SELECT
    dc.email,
    off AS manager_index,
    CONCAT(w.first_name, ' ', w.last_name) AS manager_name
  FROM
    `deduped_chain` AS dc
  CROSS JOIN
    UNNEST(dc.management_chain) AS mgr_email
    WITH OFFSET off
  LEFT JOIN
    `moz-fx-data-bq-people.composer_workday.worker` AS w
    ON w.email = mgr_email
),
manager_chain_final AS (
  SELECT
    email,
    ARRAY_AGG(manager_name ORDER BY manager_index) AS manager
  FROM
    manager_names
  GROUP BY
    email
),
final_with_name AS (
  SELECT
    mcf.manager,
    CONCAT(w.first_name, ' ', w.last_name) AS name,
    w.email
  FROM
    manager_chain_final AS mcf
  JOIN
    `moz-fx-data-bq-people.composer_workday.worker` AS w
    ON mcf.email = w.email
  WHERE
    w.currently_active = TRUE
),
bugzilla_alternates AS (
  SELECT
    fwn.manager,
    fwn.name,
    b.email AS email
  FROM
    final_with_name AS fwn
  JOIN
    `moz-fx-data-shared-prod.bugzilla_metrics.users` AS b
    ON fwn.email = b.ldap_email
  WHERE
    b.email IS NOT NULL
    AND b.email != b.ldap_email
),
combined AS (
  SELECT
    manager,
    name,
    email
  FROM
    final_with_name
  UNION ALL
  SELECT
    manager,
    name,
    email
  FROM
    bugzilla_alternates
),
deduplicated AS (
  SELECT
    manager,
    name,
    email,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY name) AS rn
  FROM
    combined
)
SELECT
  manager,
  name,
  email
FROM
  deduplicated
WHERE
  rn = 1;
