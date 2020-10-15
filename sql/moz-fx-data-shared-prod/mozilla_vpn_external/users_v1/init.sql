CREATE OR REPLACE TABLE
  users_v1
PARTITION BY
  DATE(updated_at)
AS
SELECT
  * REPLACE (TO_HEX(SHA256(fxa_uid)) AS fxa_uid)
FROM
  EXTERNAL_QUERY("moz-fx-guardian-prod-bfc7.us.guardian-sql-prod", "SELECT * FROM users")
