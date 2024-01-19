Diff the schemas of two queries. Especially useful when the BigQuery error
is truncated, and the schemas of e.g. a UNION don't match.

Use it like:
```
DECLARE res ARRAY<STRUCT<i INT64, a_col STRING, b_col STRING>>;
CALL analysis.diff_query_schemas("""SELECT * FROM a""", """SELECT * FROM b""", res);
SELECT res;
```

You'll be able to view the results of "res" to compare the schemas of the two queries, and hopefully find what doesn't match.

## Caveats
- Only compares top-level columns, no nested columns. We use the `INFORMATION_SCHEMA.COLUMNS` to get column names and position, which does not include nested columns. We can't use `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` because it does not contain ordinal positions.
- No advanced diffing: simply joins schemas on ordinal position. Feel free to file a PR to add more advanced diff capabilities.
