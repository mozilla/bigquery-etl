WITH input AS (
  SELECT AS VALUE
    """
    <paste table from README.md here>
    """
),
lines AS (
  SELECT
    ARRAY(SELECT REPLACE(TRIM(field), '*', '%') FROM UNNEST(SPLIT(line, '|')) AS field) AS fields
  FROM
    input
  CROSS JOIN
    UNNEST(SPLIT(input, '\n')) AS line
  WHERE
    line NOT LIKE '%-----%'
    AND line NOT LIKE '%canonical_name%'
),
structured AS (
  SELECT AS STRUCT
    fields[OFFSET(0)] AS app_name,
    fields[OFFSET(1)] AS normalized_os,
    fields[OFFSET(2)] AS product,
    fields[OFFSET(3)] AS canonical_name,
    fields[OFFSET(4)] AS contributes_to_2019_kpi,
    fields[OFFSET(5)] AS contributes_to_2020_kpi,
  FROM
    lines
  WHERE
    ARRAY_LENGTH(fields) > 1
),
unioned AS (
  SELECT
    *
  FROM
    structured
  -- We also want to support passing in product names as app_name in this function,
  -- so we duplicate some rows.
  UNION ALL
  SELECT
    * REPLACE (product AS app_name)
  FROM
    structured
  WHERE
    product NOT IN (SELECT app_name FROM structured)
),
formatted AS (
  SELECT
    *,
    CONCAT(
      FORMAT(
        "WHEN app_name LIKE %T AND normalized_os LIKE %T THEN STRUCT(%T AS product, %T AS canonical_name, %s AS contributes_to_2019_kpi, %s AS contributes_to_2020_kpi)",
        app_name,
        normalized_os,
        product,
        canonical_name,
        contributes_to_2019_kpi,
        contributes_to_2020_kpi
      )
    ) AS case_stmt,
  FROM
    unioned
)
SELECT
  CONCAT("CASE\n", STRING_AGG(case_stmt, "\n"), "\nELSE ('Other', 'Other', false, false) END")
FROM
  formatted
