SELECT
  IF(_update.id IS NOT NULL, _update, subscriptions_v1).*
FROM
  EXTERNAL_QUERY(
    "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    -- The external_database_query argument in EXTERNAL_QUERY must be a literal string or query
    -- parameter, and cannot be generated at runtime using function calls like CONCAT or FORMAT,
    -- so the entire value must be provided as a STRING query parameter to handle specific dates:
    --  "SELECT * FROM subscriptions WHERE DATE(updated_at) = DATE ''"
    @external_database_query
  ) AS _update
FULL JOIN
  subscriptions_v1
  USING (id)
