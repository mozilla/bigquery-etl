### Extract Keyed Scalar Sum

Takes a keyed scalar and returns a single number:
the sum of all values it contains. The expected input
type is `ARRAY<STRUCT<key STRING, value INT64>>`

The return type is `INT64`.

The `key` field will be ignored.
