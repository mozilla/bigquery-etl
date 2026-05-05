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
    AND line NOT LIKE r'%canonical\_app\_name%'
),
structured AS (
  SELECT AS STRUCT
    fields[OFFSET(0)] AS legacy_app_name,
    fields[OFFSET(1)] AS normalized_os,
    fields[OFFSET(2)] AS app_name,
    fields[OFFSET(3)] AS product,
    fields[OFFSET(4)] AS canonical_app_name,
    fields[OFFSET(5)] AS contributes_to_2019_kpi,
    fields[OFFSET(6)] AS contributes_to_2020_kpi,
    fields[OFFSET(7)] AS contributes_to_2021_kpi,
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
    * REPLACE (product AS legacy_app_name)
  FROM
    structured
  WHERE
    product NOT IN (SELECT legacy_app_name FROM structured)
),
formatted AS (
  SELECT
    *,
    CONCAT(
      FORMAT(
        "WHEN legacy_app_name LIKE %T AND normalized_os LIKE %T THEN STRUCT(%T AS app_name, %T AS product, %T AS canonical_app_name, %T AS canonical_name, %s AS contributes_to_2019_kpi, %s AS contributes_to_2020_kpi, %s AS contributes_to_2021_kpi)",
        legacy_app_name,
        normalized_os,
        app_name,
        product,
        canonical_app_name,
        canonical_app_name,
        contributes_to_2019_kpi,
        contributes_to_2020_kpi,
        contributes_to_2021_kpi
      )
    ) AS case_stmt,
  FROM
    unioned
)
SELECT
  CONCAT(
    "CASE\n",
    STRING_AGG(case_stmt, "\n"),
    "\nELSE ('other', 'Other', 'Other', 'Other', FALSE, FALSE, FALSE) END"
  )
FROM
  formatted
