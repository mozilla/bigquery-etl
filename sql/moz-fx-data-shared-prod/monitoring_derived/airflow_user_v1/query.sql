SELECT
  user.id AS user_id,
  user_role.id AS role_id,
  user.active,
  user.created_on,
  user.changed_on,
  user.last_login
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.ab_user` AS user
LEFT JOIN
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.ab_user_role` AS user_role
  ON user.id = user_role.user_id
WHERE
  NOT user._fivetran_deleted
  AND NOT user_role._fivetran_deleted
