WITH table_row_counts AS (
  SELECT
    'moz-fx-data-marketing-prod.searchconsole_addons.searchdata_site_impression' AS table_id,
    COUNT(*) AS row_count
  FROM
    `moz-fx-data-marketing-prod.searchconsole_addons.searchdata_site_impression`
  WHERE
    data_date = @date
  UNION ALL
  SELECT
    'moz-fx-data-marketing-prod.searchconsole_blog.searchdata_site_impression' AS table_id,
    COUNT(*) AS row_count
  FROM
    `moz-fx-data-marketing-prod.searchconsole_blog.searchdata_site_impression`
  WHERE
    data_date = @date
  UNION ALL
  SELECT
    'moz-fx-data-marketing-prod.searchconsole_getpocket.searchdata_site_impression' AS table_id,
    COUNT(*) AS row_count
  FROM
    `moz-fx-data-marketing-prod.searchconsole_getpocket.searchdata_site_impression`
  WHERE
    data_date = @date
  UNION ALL
  SELECT
    'moz-fx-data-marketing-prod.searchconsole_support.searchdata_site_impression' AS table_id,
    COUNT(*) AS row_count
  FROM
    `moz-fx-data-marketing-prod.searchconsole_support.searchdata_site_impression`
  WHERE
    data_date = @date
  UNION ALL
  SELECT
    'moz-fx-data-marketing-prod.searchconsole_www.searchdata_site_impression' AS table_id,
    COUNT(*) AS row_count
  FROM
    `moz-fx-data-marketing-prod.searchconsole_www.searchdata_site_impression`
  WHERE
    data_date = @date
),
empty_tables AS (
  SELECT
    ARRAY_AGG(table_id ORDER BY table_id) AS table_ids
  FROM
    table_row_counts
  WHERE
    row_count = 0
)
SELECT
  IF(
    ARRAY_LENGTH(table_ids) > 0,
    ERROR(
      CONCAT(
        'Missing data for ',
        @date,
        ' in ',
        ARRAY_LENGTH(table_ids),
        ' table(s): ',
        ARRAY_TO_STRING(table_ids, ', ')
      )
    ),
    NULL
  )
FROM
  empty_tables
