CREATE OR REPLACE TABLE
  users_v1
PARTITION BY
  DATE(updated_at)
AS
SELECT
  *
FROM
  EXTERNAL_QUERY("moz-fx-guardian-prod-bfc7.us.guardian-sql-prod", "SELECT * FROM users")
