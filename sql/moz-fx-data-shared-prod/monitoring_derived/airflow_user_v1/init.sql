CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_user_v1`
AS
SELECT
  user.id AS user_id,
  user_role.id AS role_id,
  user.active,
  user.created_on,
  user.changed_on,
  user.last_login
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.ab_user` AS user
LEFT JOIN
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.ab_user_role` AS user_role
ON
  user.id = user_role.user_id
