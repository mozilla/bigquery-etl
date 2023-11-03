/*

Extract the document version from a table name e.g. _TABLE_SUFFIX.

*/
CREATE OR REPLACE FUNCTION udf.extract_document_version(table_name STRING) AS (
  REGEXP_EXTRACT(table_name, r".*_v(.*)$")
);

-- Tests
SELECT
  mozfun.assert.equals("1", udf.extract_document_version("type_v1")),
  mozfun.assert.equals(
    "20191031",
    udf.extract_document_version("namespace__namespace_type_v20191031")
  );
