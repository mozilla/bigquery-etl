-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.normalize_metadata(metadata ANY TYPE) AS (
  mozfun.norm.metadata(metadata)
);
