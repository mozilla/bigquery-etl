This UDF extracts every parameter from a URL or bare query string.

Unlike `utils.extract_utm_from_url` there is no fixed key list and no leading `?` is
required, so unanticipated parameters are still returned. Read a single parameter out of
the result with `mozfun.map.get_key`.

Each result carries a `depth`, describing how deeply encoded the parameter was:

- `0` — a plain `key=value` pair, or a keyless token such as a bare click id
- `1` — a query string nested inside one parameter's value
- `2` — a whole string that was percent-encoded, containing no literal `=`

Where a key appears at more than one depth the shallowest occurrence wins, so
`mozfun.map.get_key` is deterministic. Values are percent-decoded. A keyless token is
returned as a key with a `NULL` value, so test for it by matching the key.
