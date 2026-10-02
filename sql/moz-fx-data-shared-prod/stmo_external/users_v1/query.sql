SELECT
  * REPLACE (SAFE.PARSE_JSON(`details`, wide_number_mode => 'round') AS `details`)
FROM
  EXTERNAL_QUERY(
    "moz-fx-data-stmo-prod-33f2.us.stmo-cloudsql-prod",
    -- api_key and password_hash are excluded
    """SELECT
         id,
         updated_at,
         created_at,
         org_id,
         name,
         email,
         groups,
         profile_image_url,
         disabled_at,
         details
       FROM
         users
    """
  )
