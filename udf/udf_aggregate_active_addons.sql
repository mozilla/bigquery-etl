CREATE TEMP FUNCTION
  udf_aggregate_active_addons(active_addons ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT element
      FROM (
        SELECT
          _n,
          FIRST_VALUE(element IGNORE NULLS) --
          OVER (PARTITION BY element.addon_id ORDER BY _n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS element
        FROM (
          SELECT
            ROW_NUMBER() OVER (PARTITION BY element.addon_id) AS _n,
            element
          FROM
            UNNEST(active_addons),
            UNNEST(list)) )
      WHERE
        _n = 1 ) AS list));
