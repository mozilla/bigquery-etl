CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.metadata_completeness_summary`
AS
SELECT
  submission_date,
  table_catalog,
  table_schema,
  table_type,
  COUNT(1) AS nbr_objects,
  SUM(
    CASE
      WHEN object_description IS NOT NULL
        AND object_description <> "\"Please provide a description for the query\""
        THEN 1
      ELSE 0
    END
  ) AS nbr_objects_with_a_description,
  SUM(
    CASE
      WHEN object_description IS NOT NULL
        AND object_description <> "\"Please provide a description for the query\""
        THEN 1
      ELSE 0
    END
  ) / COUNT(1) AS pct_objects_with_a_desc,
  SUM(
    CASE
      WHEN COALESCE(nbr_columns, 0) = COALESCE(nbr_columns_with_non_null_desc, 0)
        THEN 1
      ELSE 0
    END
  ) AS nbr_objects_where_every_col_has_a_non_null_desc,
  SUM(
    CASE
      WHEN COALESCE(nbr_columns, 0) = COALESCE(nbr_columns_with_non_null_desc, 0)
        THEN 1
      ELSE 0
    END
  ) / COUNT(1) AS pct_objects_where_every_col_has_a_non_null_desc
FROM
  `moz-fx-data-shared-prod.monitoring_derived.metadata_completeness_v1`
WHERE
  submission_date >= '2024-10-27' --first date we started recording this data
GROUP BY
  submission_date,
  table_catalog,
  table_schema,
  table_type
