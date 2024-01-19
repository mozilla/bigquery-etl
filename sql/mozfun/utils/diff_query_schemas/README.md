Diff the schemas of two queries. Especially useful when the BigQuery error
is truncated, and the schemas of e.g. a UNION don't match.

Use it like:
```
DECLARE res ARRAY<STRUCT<i INT64, differs BOOL, a_col STRING, a_data_type STRING, b_col STRING, b_data_type STRING>>;
CALL mozfun.utils.diff_query_schemas("""SELECT * FROM a""", """SELECT * FROM b""", res);
-- See entire schema entries, if you need context
SELECT res;
-- See just the elements that differ
SELECT res WHERE differs;
```

You'll be able to view the results of "res" to compare the schemas of the two queries, and hopefully find what doesn't match.
