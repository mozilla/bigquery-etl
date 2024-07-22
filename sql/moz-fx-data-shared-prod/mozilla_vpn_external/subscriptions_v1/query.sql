{% if is_init() %}
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
{% else %}
  SELECT
    IF(_update.id IS NOT NULL, _update, subscriptions_v1).*
  FROM
    EXTERNAL_QUERY(
      "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    -- The external_database_query argument in EXTERNAL_QUERY must be a literal string or query
    -- parameter, and cannot be generated at runtime using function calls like CONCAT or FORMAT,
    -- so the entire value must be provided as a STRING query parameter to handle specific dates:
    -- {% raw %} "SELECT * FROM subscriptions WHERE DATE(updated_at) = DATE '{{ds}}'" {% endraw %}
      @external_database_query
    ) AS _update
  FULL JOIN
    `moz-fx-data-shared-prod.mozilla_vpn_external.subscriptions_v1` AS subscriptions_v1
    USING (id)
{% endif %}
