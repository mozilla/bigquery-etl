### Extract Keyed Histogram Sum

Takes a keyed histogram and returns a single number:
the sum of all keys it contains. The expected input
type is `ARRAY<STRUCT<key STRING, value STRING>>`

The return type is `INT64`.

The `key` field will be ignored, and
the `value is expected to be the compact
histogram representation.
