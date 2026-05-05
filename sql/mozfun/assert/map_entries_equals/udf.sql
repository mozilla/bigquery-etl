CREATE OR REPLACE FUNCTION assert.map_entries_equals(expected ANY TYPE, actual ANY TYPE)
RETURNS BOOLEAN AS (
  (
    SELECT
      LOGICAL_OR(
        IF(
          e.value != a.value
          OR (e.value IS NULL AND a.value IS NOT NULL)
          OR (e.value IS NOT NULL AND a.value IS NULL),
          assert.error('map entry', e, a),
          TRUE
        )
      )
    FROM
      UNNEST(actual) AS a
    FULL OUTER JOIN
      -- BQ does not allow full array scans with FULL join
      -- so we trick it using a subquery
      (SELECT * FROM UNNEST(expected)) AS e
      USING (key)
  )
);
