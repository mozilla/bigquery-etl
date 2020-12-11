# json

Functions for parsing Mozilla-specific JSON data types.


## mode_last (UDF)

Returns the most frequently occuring element in an array of json-compatible elements. In the case of multiple values tied for the highest count, it returns the value that appears latest in the array. Nulls are ignored.



## extract_int_map (UDF)

Returns an array of key/value structs from a string representing a JSON map. Both keys and values are cast to integers.
This is the format for the "values" field in the desktop telemetry histogram JSON representation.

