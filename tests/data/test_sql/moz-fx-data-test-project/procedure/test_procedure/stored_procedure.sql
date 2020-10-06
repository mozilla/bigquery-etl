CREATE OR REPLACE PROCEDURE
  procedure.test_procedure(out STRING)
BEGIN
  SET out = mozfun.json.mode_last('{}');
END;
