CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.zoom.users`
AS
SELECT
  id,
  created_at,
  dept,
  email,
  first_name,
  languages AS `language`,
  last_client_version,
  last_login_time,
  last_name,
  pmi,
  role_id,
  status,
  timezone,
  type,
  verified,
  _fivetran_deleted AS is_deleted,
FROM
  `moz-fx-data-bq-fivetran.zoom.users`
