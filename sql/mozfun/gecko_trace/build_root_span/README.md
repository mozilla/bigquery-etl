# gecko_trace.build_root_span

Builds a root span tree structure from an array of span objects.

## Signature

```sql
gecko_trace.build_root_span(spans ARRAY<JSON>) RETURNS JSON
```

## Arguments

- `spans`: Array of JSON objects representing individual spans. Each span should
  contain at minimum:
  - `span_id`: Unique identifier for the span
  - `parent_span_id`: ID of the parent span (null for root spans)

## Description

Takes an array of JSON span objects and constructs a hierarchical tree structure
by linking spans with their parent-child relationships. 

If no explicit root span is found, the function will attempt to find a single
"missing" root span. If there are multiple or no missing roots, an error is
thrown.

## Returns

Returns a JSON object representing the root span with all child spans nested in
`childSpans` arrays throughout the tree structure.

## Example

```sql
SELECT gecko_trace.build_root_span([
  JSON '{"span_id": "root", "parent_span_id": null, "name": "main_process"}',
  JSON '{"span_id": "child1", "parent_span_id": "root", "name": "network_request"}',
  JSON '{"span_id": "child2", "parent_span_id": "root", "name": "dom_parse"}',
  JSON '{"span_id": "grandchild", "parent_span_id": "child1", "name": "dns_lookup"}'
])
```

This would return a tree structure where the root span contains two child spans
in its `childSpans` array, and one of those children has its own child span.
