CREATE TEMP FUNCTION udf_search_counts(search_counts ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      SUBSTR(_key, 0, pos - 2) AS engine,
      SUBSTR(_key, pos) AS source,
      udf_json_extract_histogram(value).sum AS `count`
    FROM
      UNNEST(search_counts),
      UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
      UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
  )
);
