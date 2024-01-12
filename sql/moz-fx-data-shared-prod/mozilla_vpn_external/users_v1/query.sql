SELECT
  IF(_current.id IS NULL, _previous, _current).* REPLACE (
    CAST(
      NULL AS STRING
    ) AS fxa_profile_json, -- contains unhashed fxa_uid
    IF(_current.id IS NULL, _previous.fxa_uid, TO_HEX(SHA256(_current.fxa_uid))) AS fxa_uid
  )
FROM
  EXTERNAL_QUERY(
    "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    -- The external_database_query argument in EXTERNAL_QUERY must be a literal string or query
    -- parameter, and cannot be generated at runtime using function calls like CONCAT or FORMAT,
    -- so the entire value must be provided as a STRING query parameter to handle specific dates:
    --  "SELECT * FROM users WHERE DATE(updated_at) = DATE ''"
    @external_database_query
  ) AS _current
FULL JOIN
  users_v1 AS _previous
  USING (id)
