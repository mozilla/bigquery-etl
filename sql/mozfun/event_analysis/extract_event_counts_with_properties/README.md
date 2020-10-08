### Usage
```
extract_event_counts_with_properties(
    events STRING
)
```

`events` - A comma-separated events string,
where each event is represented as a string
of unicode chars.

### Example
See [this query](https://sql.telemetry.mozilla.org/queries/75082)
for example usage.

### Caveats
This function extracts both counts for events with each property,
and for all events without their properties.

This allows us to include both total counts for an event (with any
property value), and events that don't have properties.
