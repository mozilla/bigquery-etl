/*

Extract the document version from a table name e.g. _TABLE_SUFFIX.

*/
CREATE TEMP FUNCTION udf_extract_document_version(table_name STRING) AS (REGEXP_EXTRACT(table_name, r".*_v(.*)$"));
-- Tests
SELECT
  assert_equals("1", udf_extract_document_version("type_v1")),
  assert_equals(
    "20191031",
    udf_extract_document_version("namespace__namespace_type_v20191031")
  );
