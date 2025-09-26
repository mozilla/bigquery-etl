CREATE OR REPLACE FUNCTION gecko_trace.calculate_signature(rootSpan JSON)
RETURNS STRING
LANGUAGE js AS r"""
  // cyrb53 (c) 2018 bryc (github.com/bryc). License: Public domain. Attribution appreciated.
  // A fast and simple 64-bit (or 53-bit) string hash function with decent collision resistance.
  // Largely inspired by MurmurHash2/3, but with a focus on speed/simplicity.
  // See https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript/52171480#52171480
  // https://github.com/bryc/code/blob/master/jshash/experimental/cyrb53.js
  const cyrb64 = (str, seed = 0) => {
    let h1 = 0xdeadbeef ^ seed,
    h2 = 0x41c6ce57 ^ seed;
    for (let i = 0, ch; i < str.length; i++) {
      ch = str.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
    h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
    h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    // For a single 53-bit numeric return value we could return
    // 4294967296 * (2097151 & h2) + (h1 >>> 0);
    // but we instead return the full 64-bit value:
    return [h2 >>> 0, h1 >>> 0];
  };

  const seed = 0;
  let digest = "";
  const hash = (str) => {
    const [h2, h1] = cyrb64(digest + str, seed);
    digest =
      h2.toString(36).padStart(7, "0") + h1.toString(36).padStart(7, "0");
  };

  const ATTRS_TO_SKIP = {"gecko_process_internal_id": null}
  const hashAttrs = (attrs) => {
    for (const [key, value] of Object.entries(attrs)) {
      if (key in ATTRS_TO_SKIP) continue;
      hash(key);
      hash(value);
    }
  }

  const hashEvents = (events) => {
    for (const event of events) {
      hash(event.name);
      hashAttrs(event.attributes);
    }
  };

  const stack = [rootSpan];
  while (stack.length > 0) {
    const span = stack.pop();
    hashAttrs(span.resource.attributes);
    hash(span.scope.name);
    hash(span.name);
    if (span.events) {
        hashEvents(span.events);
    }
    stack.push(...span.childSpans);
  }

  return digest;
""";

-- Tests
SELECT
  -- Test with simple root span
  assert.not_null(
    gecko_trace.calculate_signature(
      JSON '{"span_id": "root", "name": "test", "scope": {"name": "test_scope"}, "resource": {"attributes": {}}, "childSpans": []}'
    )
  ),
  -- Test that same input produces same signature
  assert.equals(
    gecko_trace.calculate_signature(
      JSON '{"span_id": "root", "name": "test", "scope": {"name": "test_scope"}, "resource": {"attributes": {}}, "childSpans": []}'
    ),
    gecko_trace.calculate_signature(
      JSON '{"span_id": "root", "name": "test", "scope": {"name": "test_scope"}, "resource": {"attributes": {}}, "childSpans": []}'
    )
  ),
  -- Test that null input returns empty string
  assert.equals("", gecko_trace.calculate_signature(NULL));
