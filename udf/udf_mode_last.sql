CREATE TEMP FUNCTION udf_mode_last(x ANY TYPE) AS ((
  SELECT
    val
  FROM (
    SELECT
      val,
      COUNT(val) AS n,
      MAX(offset) AS max_offset
    FROM
      UNNEST(x) AS val
    WITH OFFSET AS offset
    GROUP BY
      val
    ORDER BY
      n DESC,
      max_offset DESC
  )
  LIMIT 1
));

/*

Returns the most frequently occuring element in an array.

In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are ignored.

Examples:

SELECT udf_mode_last(['foo', 'bar', 'baz', 'bar', 'fred']);
-- bar

SELECT udf_mode_last(['foo', 'bar', 'baz', 'bar', 'baz', 'fred']);
-- baz

SELECT udf_mode_last([null, 'foo', null]);
-- foo

*/
