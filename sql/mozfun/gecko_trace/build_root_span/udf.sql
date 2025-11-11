CREATE OR REPLACE FUNCTION gecko_trace.build_root_span(spans ARRAY<JSON>)
RETURNS JSON
LANGUAGE js AS r"""
  const spansById = new Map();
  let rootSpanId;

  spans.forEach((span) => {
    const spanId = span.span_id;
    // Re-attach any children accumulated while parent was "missing"
    const maybeMissingSelf = spansById.get(spanId);
    span.childSpans = maybeMissingSelf?.childSpans ?? [];
    spansById.set(spanId, span);

    if (!span.parent_span_id) {
      rootSpanId = spanId; // yay, we found the root span
      return;
    }

    const parent = spansById.get(span.parent_span_id) || {
      span_id: span.parent_span_id,
      childSpans: [],
      type: "missing",
    };
    parent.childSpans.push(span);
    spansById.set(span.parent_span_id, parent);
  });

  if (!rootSpanId) {
    // Find the single missing root, if any
    const missingRoots = Array.from(spansById.values()).filter(
      (span) => span.type == "missing",
    );
    if (missingRoots.length != 1) {
      throw new Error(
        `Unable to construct span tree: expected exactly one missing root span, but found ${missingRoots.length}`,
      );
    }

    rootSpanId = missingRoots[0].span_id;
  }

  return spansById.get(rootSpanId);
""";

-- Tests
SELECT
  -- Test with simple parent-child relationship
  assert.not_null(
    gecko_trace.build_root_span(
      [
        JSON '{"span_id": "root", "parent_span_id": null, "name": "root_span"}',
        JSON '{"span_id": "child1", "parent_span_id": "root", "name": "child_span"}'
      ]
    )
  ),
  -- Test with empty array
  assert.null(gecko_trace.build_root_span([])),
  -- Test single span (should be root)
  assert.equals(
    "root",
    JSON_VALUE(
      gecko_trace.build_root_span(
        [JSON '{"span_id": "root", "parent_span_id": null, "name": "root_span"}']
      ),
      "$.span_id"
    )
  );
