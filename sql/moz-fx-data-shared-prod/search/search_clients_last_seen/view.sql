CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_clients_last_seen`
AS
SELECT
  * REPLACE (
    ARRAY(
      SELECT AS STRUCT
        COALESCE(udf.normalize_search_engine(key), "Other") AS key,
        value
      FROM
        UNNEST(engine_searches)
    ) AS engine_searches
  )
FROM
  `moz-fx-data-shared-prod.search.search_clients_last_seen_v1`,
  UNNEST(engine_searches)
