# map

Functions for working with arrays of key/value structs.


## get_key_with_null (UDF)

Fetch the value associated with a given key from an array of key/value structs.
Because map types aren't available in BigQuery, we model maps as arrays of structs instead, and this function provides map-like access to such fields. This version matches NULL keys as well.



## mode_last (UDF)

Combine entries from multiple maps, determine the value for each key using mozfun.stats.mode_last.



## sum (UDF)

Return the sum of values by key in an array of map entries. The expected schema for entries is ARRAY<STRUCT<key ANY TYPE, value ANY TYPE>>, where the type for value must be supported by SUM, which allows numeric data types INT64, NUMERIC, and FLOAT64.



## get_key (UDF)

Fetch the value associated with a given key from an array of key/value structs.
Because map types aren't available in BigQuery, we model maps as arrays of structs instead, and this function provides map-like access to such fields.

