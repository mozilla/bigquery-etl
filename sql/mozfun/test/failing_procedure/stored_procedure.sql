-- choose a simple function
CREATE OR REPLACE PROCEDURE
  test.stored_procedure_norm_os(IN value STRING, OUT res STRING)
BEGIN
  SELECT
    SET res = (SELECT mozfun.norm.os(value));
END;
BEGIN
  DECLARE res STRING;

  CALL test.stored_procedure_norm_os("Windows", res);

  SELECT
    assert_equals(res, mozfun.norm.os("Windows"));
END
