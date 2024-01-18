CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_user`
AS
WITH user_role_mapping AS (
  SELECT
    *
  FROM
    UNNEST(
      [
        STRUCT(1 AS role_id, 'Admin' AS role_name),
        STRUCT(2, 'Public'),
        STRUCT(3, 'Viewer'),
        STRUCT(4, 'User'),
        STRUCT(5, 'Op'),
        STRUCT(6, 'Fx Usage Report Editor')
      ]
    )
)
SELECT
  role_name AS user_role,
  users.* EXCEPT (role_id, active)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_user_v1` AS users
LEFT JOIN
  user_role_mapping
  USING (role_id)
WHERE
  active
