CREATE OR REPLACE PROCEDURES procedure.test_procedure(out STRING) 
BEGIN 
  SET out = mozfun.json.parse('{}');
END;
