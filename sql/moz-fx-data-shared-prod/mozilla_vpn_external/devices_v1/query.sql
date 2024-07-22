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
      name,
      mullvad_id,
      pubkey,
      ipv4_address,
      ipv6_address,
      created_at,
      updated_at,
      uid,
      platform,
      useragent,
      unique_id
    FROM devices
    """
    )
{% else %}
  SELECT
    IF(_update.id IS NOT NULL, _update, devices_v1).*
  FROM
    EXTERNAL_QUERY(
      "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    -- The external_database_query argument in EXTERNAL_QUERY must be a literal string or query
    -- parameter, and cannot be generated at runtime using function calls like CONCAT or FORMAT,
    -- so the entire value must be provided as a STRING query parameter to handle specific dates:
    -- {% raw %} "SELECT * FROM devices WHERE DATE(updated_at) = DATE '{{ds}}'" {% endraw %}
      @external_database_query
    ) AS _update
  FULL JOIN
    `moz-fx-data-shared-prod.mozilla_vpn_external.devices_v1` AS devices_v1
    USING (id)
{% endif %}
