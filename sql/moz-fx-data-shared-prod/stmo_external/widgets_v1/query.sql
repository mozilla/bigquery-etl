SELECT
  * REPLACE (SAFE.PARSE_JSON(`options`, wide_number_mode => 'round') AS `options`)
FROM
  EXTERNAL_QUERY(
    "moz-fx-data-stmo-prod-33f2.us.stmo-cloudsql-prod",
    """SELECT
         id,
         updated_at,
         created_at,
         visualization_id,
         text,
         width,
         options,
         dashboard_id
       FROM
         widgets
    """
  )
