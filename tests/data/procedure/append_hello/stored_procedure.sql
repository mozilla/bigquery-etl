CREATE OR REPLACE PROCEDURE
  procedure.append_hello(out STRING)
BEGIN
  CALL procedure.test_procedure(out);
END;
BEGIN
  SELECT
    udf.test_shift_28_bits_one_day();

  DECLARE a STRING DEFAULT '';

  CALL procedure.append_hello(a);

  SELECT
    assert_equals('hello', a);
END;
