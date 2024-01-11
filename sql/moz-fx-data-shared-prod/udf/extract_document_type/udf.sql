/*

Extract the document type from a table name e.g. _TABLE_SUFFIX.

*/
CREATE OR REPLACE FUNCTION udf.extract_document_type(table_name STRING) AS (
  REGEXP_EXTRACT(table_name, r"^(.*)_v.*")
);

-- Tests
SELECT
  mozfun.assert.equals("type", udf.extract_document_type("type_v1")),
  mozfun.assert.equals(
    "namespace__namespace_type",
    udf.extract_document_type("namespace__namespace_type_v20191031")
  );
