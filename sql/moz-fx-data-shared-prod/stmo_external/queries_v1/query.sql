SELECT
  * REPLACE (
    SAFE.PARSE_JSON(`options`, wide_number_mode => 'round') AS `options`,
    SAFE.PARSE_JSON(`schedule`, wide_number_mode => 'round') AS `schedule`
  )
FROM
  EXTERNAL_QUERY(
    "moz-fx-data-stmo-prod-33f2.us.stmo-cloudsql-prod",
    -- api_key is excluded because it can be used to run the query as its owner
    """SELECT
         id,
         updated_at,
         created_at,
         org_id,
         data_source_id,
         latest_query_data_id,
         name,
         description,
         query,
         query_hash,
         user_email,
         user_id,
         last_modified_by_id,
         is_archived,
         options,
         version,
         is_draft,
         schedule_failures,
         tags,
         schedule
       FROM
         queries
    """
  )
