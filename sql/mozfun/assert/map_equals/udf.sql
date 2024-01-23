CREATE OR REPLACE FUNCTION assert.map_equals(expected ANY TYPE, actual ANY TYPE)
RETURNS BOOLEAN AS (
  IF(
    EXISTS(
      SELECT
        key
      FROM
        UNNEST(expected) AS e
      FULL OUTER JOIN
          -- BQ does not allow full array scans with FULL join
          -- so we trick it using a subquery
        (SELECT * FROM UNNEST(actual)) AS a
        USING (key)
      WHERE
        e.value != a.value
        OR (e.value IS NULL AND a.value IS NOT NULL)
        OR (e.value IS NOT NULL AND a.value IS NULL)
    ),
    assert.error('map', expected, actual),
    TRUE
  )
);
