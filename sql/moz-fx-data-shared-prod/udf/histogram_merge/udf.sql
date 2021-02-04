-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.histogram_merge(histogram_list ANY TYPE) AS (
  mozfun.hist.merge(histogram_list)
);
