This UDF percent-decodes a string. BigQuery has no native URL decode, so escapes are
tokenised and converted with `FROM_HEX`.

Consecutive escapes are grouped into a single run before decoding, so a multi-byte UTF-8
sequence such as `%C3%A9` becomes `é` rather than two invalid bytes. A stray `%` that
begins no valid escape is kept as it arrived, and `+` is left as a literal `+` rather
than decoded to a space, so base64 values survive intact.
