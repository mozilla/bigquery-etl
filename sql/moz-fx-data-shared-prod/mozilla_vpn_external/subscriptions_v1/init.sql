CREATE OR REPLACE TABLE
  subscriptions_v1
PARTITION BY
  DATE(updated_at)
AS
SELECT
  *
FROM
  EXTERNAL_QUERY(
    "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    """
    SELECT
      id,
      user_id,
      is_active,
      mullvad_token,
      mullvad_account_created_at,
      mullvad_account_expiration_date,
      ended_at,
      created_at,
      updated_at,
      type,
      fxa_last_changed_at,
      fxa_migration_note
    FROM subscriptions
    """
  )
