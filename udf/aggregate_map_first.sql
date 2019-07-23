CREATE TEMP FUNCTION
  udf_aggregate_map_first(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT * EXCEPT (_n)
      FROM (
        SELECT
          * EXCEPT (value),
          FIRST_VALUE(value IGNORE NULLS) --
          OVER (PARTITION BY key ORDER BY _n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS value
        FROM (
          SELECT
            ROW_NUMBER() OVER (PARTITION BY key) AS _n,
            key,
            value
          FROM
            UNNEST(maps),
            UNNEST(key_value)) )
      WHERE
        _n = 1 ) AS key_value));
