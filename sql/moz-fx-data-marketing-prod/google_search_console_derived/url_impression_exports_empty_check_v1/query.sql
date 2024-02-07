{% set gsc_export_dataset_ids = [
    'moz-fx-data-marketing-prod.searchconsole_addons',
    'moz-fx-data-marketing-prod.searchconsole_blog',
    'moz-fx-data-marketing-prod.searchconsole_getpocket',
    'moz-fx-data-marketing-prod.searchconsole_support',
    'moz-fx-data-marketing-prod.searchconsole_www',
] %}
WITH table_row_counts AS (
  {% for gsc_export_dataset_id in gsc_export_dataset_ids %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      '{{ gsc_export_dataset_id }}.searchdata_url_impression' AS table_id,
      COUNT(*) AS row_count
    FROM
      `{{ gsc_export_dataset_id }}.searchdata_url_impression`
    WHERE
      data_date = @date
  {% endfor %}
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
