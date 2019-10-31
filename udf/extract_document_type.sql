/*

Extract the document type from a table name e.g. _TABLE_SUFFIX.

*/
CREATE TEMP FUNCTION udf_extract_document_type(table_name STRING) AS (REGEXP_EXTRACT(_TABLE_SUFFIX, r "^(.*)_v.*"));
-- Tests
SELECT
  assert_equals("type", udf_extract_document_type("type_v1")),
  assert_equals(
    "namespace__namespace_type",
    udf_extract_document_type("namespace__namespace_type_v20191101")
  );
