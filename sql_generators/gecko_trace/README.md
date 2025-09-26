# Gecko Trace SQL Generator

This generator creates BigQuery tables and views for processing Gecko trace
telemetry data from Firefox applications.

## Overview

The generator creates a complete data pipeline for analyzing Gecko traces:

1. **Derived Tables** (`{dataset}_derived`): Process raw telemetry into
   structured span and trace data
2. **Aggregate Views** (`gecko_trace_aggregates`): Unified views combining data
   across all Firefox applications

## Generated Tables

### Derived Tables (per Firefox application)

For each Firefox application (`firefox_desktop`, `org_mozilla_fenix_nightly`,
`org_mozilla_firefox_beta`), the generator creates:

#### `gecko_trace_spans_v1`

- **Purpose**: Individual spans extracted from raw traces
- **Schema**: Flattened span data with trace/parent relationships, timing,
  events, and metadata
- **Source**: Raw telemetry `traces` table

#### `gecko_trace_traces_v1`

- **Purpose**: Complete traces with hierarchical span structures
- **Schema**: Aggregated traces with `root_span` JSON tree and calculated
  `signature` hash
- **Dependencies**: Uses `mozfun.gecko_trace.build_root_span()` and
  `mozfun.gecko_trace.calculate_signature()`
- **Source**: `gecko_trace_spans_v1` table

#### `gecko_trace_signatures_v1`

- **Purpose**: Statistics grouped by trace signature
- **Schema**: Signature hash, average duration, and hit counts
- **Source**: `gecko_trace_traces_v1` table

### Aggregate Views

Located in `moz-fx-data-shared-prod.gecko_trace_aggregates`:

#### `spans`

- Unified view of all span data across Firefox applications
- UNION ALL of all `gecko_trace_spans_v1` tables

#### `traces`

- Unified view of all trace data across Firefox applications
- UNION ALL of all `gecko_trace_traces_v1` tables

#### `signatures`

- Unified view of all signature statistics across Firefox applications
- UNION ALL of all `gecko_trace_signatures_v1` tables

## Usage

```bash
# Generate all tables and views with default settings
./bqetl generate gecko_trace

# Specify custom output directory and target project
./bqetl generate gecko_trace \
    --output-dir /path/to/output \
    --target-project my-project-id
```

### Options

- `--output-dir`: Directory where generated SQL files are written (default:
  `sql`)
- `--target-project`: BigQuery project ID for generated queries (default:
  `moz-fx-data-shared-prod`)

## Generated File Structure

```
<output-dir>/
├── <target-project>/
│   ├── <per-app-id>_derived/
│   │   ├── gecko_trace_spans_v1/
│   │   │   ├── query.sql
│   │   │   ├── metadata.yaml
│   │   │   └── schema.yaml
│   │   ├── gecko_trace_traces_v1/
│   │   └── gecko_trace_signatures_v1/
│   └── gecko_trace_aggregates/
│       ├── dataset_metadata.yaml
│       ├── spans/
│       │   ├── view.sql
│       │   ├── metadata.yaml
│       │   └── schema.yaml
│       ├── traces/
│       └── signatures/
```

## Data Flow

```
Raw Telemetry (gecko_trace ping table)
    ↓
gecko_trace_spans_v1 (individual spans)
    ↓
gecko_trace_traces_v1 (complete traces with root_span + signature)
    ↓
gecko_trace_signatures_v1 (signature statistics)
    ↓
gecko_trace_aggregates.* (unified views across applications)
```

## Example Queries

### Analyze trace signatures across applications

```sql
SELECT
  application,
  signature,
  average_duration_nano / 1000000 as avg_duration_ms,
  hits
FROM `moz-fx-data-shared-prod.gecko_trace_aggregates.signatures`
WHERE hits > 100
ORDER BY average_duration_nano DESC
```

### Examine span hierarchy for a specific trace

```sql
SELECT
  JSON_EXTRACT_SCALAR(root_span, '$.name') as root_name,
  JSON_EXTRACT_ARRAY(root_span, '$.childSpans') as children,
  duration_nano / 1000000 as duration_ms
FROM `moz-fx-data-shared-prod.gecko_trace_aggregates.traces`
WHERE trace_id = 'your-trace-id-here'
```

## Configuration

The generator processes data for these Firefox applications:

- `firefox_desktop` - Firefox Desktop
- `org_mozilla_fenix_nightly` - Firefox for Android (Nightly)
- `org_mozilla_firefox_beta` - Firefox for Android (Beta)

To add additional applications, update the `APPLICATIONS ` list in
`__init__.py`.

## Templates

The generator uses Jinja2 templates located in `templates/`:

- `{database}_derived/` - Templates for derived tables (per application)
- `moz-fx-data-shared-prod/gecko_trace_aggregates/` - Templates for aggregate
  views

All templates include proper metadata, schemas, and documentation.
