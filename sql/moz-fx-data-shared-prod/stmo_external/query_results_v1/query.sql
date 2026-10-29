SELECT
  *
FROM
  EXTERNAL_QUERY(
    "moz-fx-data-stmo-prod-33f2.us.stmo-cloudsql-prod",
    -- data is excluded because it holds the result rows, which can come from
    -- data sources with restricted access
    """SELECT
         id,
         org_id,
         data_source_id,
         query_hash,
         query,
         runtime,
         retrieved_at
       FROM
         query_results
    """
  )
