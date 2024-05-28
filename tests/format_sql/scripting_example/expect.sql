CREATE PROCEDURE
  mydataset.select_from_tables_and_append(
    target_date DATE,
    OUT rows_added INT64,
    INOUT bidirectional INT64,
  )
BEGIN
  BEGIN TRANSACTION;

  CREATE TEMP TABLE
    data_for_target_date
  AS
  SELECT
    t1.id,
    t1.x,
    t2.y
  FROM
    dataset.partitioned_table1 AS t1
  JOIN
    dataset.partitioned_table2 AS t2
    ON t1.id = t2.id
  WHERE
    t1.date = target_date
    AND t2.date = target_date;

  SET rows_added = (SELECT COUNT(*) FROM data_for_target_date);

  SELECT
    id,
  FROM
    data_for_target_date;

  DROP TABLE
    data_for_target_date;

  WHILE
    expr
  DO
    IF
      expr1
    THEN
      SELECT
        field1;

      SELECT
        field2;
    ELSEIF
      expr2
    THEN
      RETURN;
    ELSE
      BREAK;
    END IF;
  END WHILE;

  LOOP
    LEAVE;
  END LOOP;

  COMMIT TRANSACTION;
END;
