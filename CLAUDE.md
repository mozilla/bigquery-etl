# BigQuery ETL Project Instructions

This document provides guidelines for generating BigQuery ETL files in the Mozilla bigquery-etl project.

## Project Overview

This is a Mozilla bigquery-etl project that manages BigQuery table definitions, queries, and associated metadata. Similar to dbt, we maintain query definitions with associated metadata and schemas.

Each table/query typically consists of three files:
- `query.sql` OR `query.py` - The query definition (SQL or Python)
- `metadata.yaml` - Metadata about scheduling, ownership, and dependencies
- `schema.yaml` - BigQuery schema definition with field types and descriptions

**Query types:**
- **query.sql**: Standard SQL queries (most common, ~95% of tables)
- **query.py**: Python scripts for complex ETL operations that require:
  - API calls to external services
  - Complex data transformations using pandas/Python libraries
  - Multi-project queries or INFORMATION_SCHEMA operations
  - Custom logic that's difficult to express in SQL

## Directory Structure

```
sql/{project}/{dataset}/{table_name}/
├── query.sql OR query.py
├── metadata.yaml
└── schema.yaml
```

### Project Naming Conventions

Primary production project: `moz-fx-data-shared-prod`

Other projects follow pattern: `moz-fx-{product}-{environment}`
- Examples: `moz-fx-cjms-nonprod-9a36`, `moz-fx-data-marketing-prod`

Special projects:
- `mozfun` - User-defined functions (UDFs) library

### Dataset Naming Patterns

Dataset names indicate their purpose through suffixes:

**By suffix:**
- `{name}_derived` - Processed/transformed data (most common for ETL outputs)
- `{name}_external` - External data sources synced into BigQuery
- `{name}_syndicate` - Syndicated/shared data
- `{name}` (no suffix) - Raw ping data or stable tables

**Common dataset prefixes by product/source:**
- `telemetry_*` - Firefox telemetry data (e.g., `telemetry_derived`, `telemetry_stable`)
- `accounts_*` - Firefox Accounts/FxA data
- `contextual_services_*` - Contextual services data
- `ads_*` - Advertising data
- Product-specific: `activity_stream`, `addons`, `default_browser_agent`, etc.
- External services: `braze_*`, `adjust_*`, `acoustic_*`, `apple_ads_*`, etc.

**Special datasets:**
- `analysis` - Ad-hoc analysis tables
- `backfills_staging_derived` - Staging area for backfills
- `mozfun` - User-defined functions

**Reference:** See `../data-docs/src/datasets/` for detailed dataset documentation

### Table Versioning Patterns

Tables use version suffixes:
- `{table_name}_v1` - Initial version
- `{table_name}_v2` - Second version (breaking changes)
- Increment version when making breaking schema changes

**Directory structure:**
- Always flat: `sql/{project}/{dataset}/{table_name}/`
- Never use subdirectories within table directories

**Incremental vs Full Refresh:**
- **Incremental queries**: Add/update one partition at a time
  - Must accept `@submission_date` parameter
  - Must output a `submission_date` column matching the parameter
  - Labeled with `incremental: true` in metadata.yaml
  - Use `WRITE_TRUNCATE` mode to replace partitions atomically
  - Example: `clients_daily_v1` adds one day's partition per run
- **Full refresh queries**: Rewrite entire table on each run
  - No `@submission_date` parameter needed
  - Set `date_partition_parameter: null` in metadata.yaml scheduling section
  - Example: snapshot tables or aggregates across all time

## File Templates

### query.sql

SQL queries should follow these conventions:

**SQL Formatting:**
- Use uppercase for SQL keywords (SELECT, FROM, WHERE, GROUP BY, etc.)
- Use 2-space indentation
- Place each field on its own line in SELECT statements
- Align major clauses (SELECT, FROM, WHERE, GROUP BY) at the same indentation level

**CTE Usage:**
- Use CTEs (WITH statements) for complex queries to improve readability
- Name CTEs descriptively (e.g., `base`, `filtered_events`, `daily_aggregates`)
- Break complex logic into multiple CTEs rather than nested subqueries

**Partitioning:**
- Always use partition filters for partitioned tables: `WHERE submission_date = @submission_date`
- Use date parameters like `@submission_date` for daily scheduled queries
- Common partition fields: `submission_date`, `submission_timestamp`

**Date/Time Handling:**
- Use `DATE(submission_timestamp)` to convert timestamps to dates
- Reference dates using parameters: `@submission_date`, `@date`
- Use `SAFE.TIMESTAMP_MILLIS()` for safe timestamp conversions

**Naming Conventions:**
- Use snake_case for all column names
- Prefix UDF calls with project: `moz-fx-data-shared-prod.udf.function_name` or use `mozfun.` for mozfun UDFs
- Use descriptive CTE names that indicate purpose

**Jinja Templating:**
- Queries are interpreted as [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templates before execution
- The bqetl CLI renders Jinja templates into SQL before submitting to BigQuery
- Common Jinja patterns:
  - `{% if is_init() %}` - Code for initial table population (full backfill)
  - `{% else %}` - Code for incremental updates
  - `{% endif %}` - End conditional block
  - `{{ metrics.calculate() }}` - Reference metric-hub metrics
  - `{{ metrics.data_source() }}` - Reference metric-hub data sources
- The `is_init()` function differentiates between initialization and incremental runs:
  ```sql
  {% if is_init() %}
    -- Full historical backfill logic
    AND submission_date BETWEEN '2020-01-01' AND @submission_date
  {% else %}
    -- Incremental logic for single partition
    AND submission_date = @submission_date
  {% endif %}
  ```
- Use `./bqetl query initialize` to run the `is_init()` branch
- Regular scheduled runs use the `else` branch

**Header Comments:**
- Start with a brief comment explaining the query's purpose
- Reference bug/ticket numbers when applicable (e.g., `-- See https://bugzilla.mozilla.org/show_bug.cgi?id=123456`)
- Document any data filtering or exclusions with inline comments

**Example simple query:**
```sql
-- Daily per-client aggregation of event counts
SELECT
  submission_date,
  sample_id,
  client_id,
  COUNT(*) AS n_total_events,
  COUNTIF(event_category = 'example') AS n_example_events,
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  sample_id,
  client_id
```

**Example with CTE:**
```sql
-- Creates a flattened events dataset
WITH base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    normalized_channel,
    event_category,
    event_method,
  FROM
    `moz-fx-data-shared-prod.telemetry.event`
  WHERE
    DATE(submission_timestamp) = @submission_date
)
SELECT
  *
FROM
  base
WHERE
  event_category IS NOT NULL
```

**Reference examples:**
- Simple query: `sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/query.sql`
- Aggregation: `sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_event_v1/query.sql`
- Complex with CTEs: `sql/moz-fx-data-shared-prod/telemetry_derived/event_events_v1/query.sql`

### query.py

Python scripts are used for complex ETL operations that cannot be easily expressed in SQL.

**When to use query.py instead of query.sql:**
- Fetching data from external APIs (e.g., Bigeye, Stripe, external services)
- Complex data transformations using pandas or other Python libraries
- Operations requiring multiple BigQuery clients or cross-project queries
- Accessing BigQuery INFORMATION_SCHEMA across multiple projects
- Custom business logic that's clearer in Python than SQL

**Standard structure:**
```python
"""
Module docstring explaining what this script does.
"""

from argparse import ArgumentParser
from google.cloud import bigquery

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="dataset_name")
parser.add_argument("--destination_table", default="table_name_v1")

def main():
    """Main function that performs the ETL operation."""
    args = parser.parse_args()

    client = bigquery.Client(args.project)

    # Your ETL logic here
    # Common patterns:
    # 1. Query BigQuery and transform data
    # 2. Fetch from external API
    # 3. Load data to BigQuery with write_disposition

    destination_table = f"{args.project}.{args.destination_dataset}.{args.destination_table}"

    # Example: WRITE_TRUNCATE for full refresh, WRITE_APPEND for incremental
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_TRUNCATE"
    )

    client.query(query, job_config=job_config).result()

if __name__ == "__main__":
    main()
```

**Common patterns:**

1. **Multi-project INFORMATION_SCHEMA query:**
```python
DEFAULT_PROJECTS = ["mozdata", "moz-fx-data-shared-prod"]

for project in args.source_projects:
    client = bigquery.Client(project)
    query = create_query(project)
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_APPEND"  # Append results from each project
    )
    client.query(query, job_config=job_config).result()
```

2. **External API to BigQuery:**
```python
import pandas as pd
import requests

def fetch_api_data() -> pd.DataFrame:
    """Fetch data from external API."""
    response = requests.get(API_URL, headers=headers)
    response.raise_for_status()
    return pd.json_normalize(response.json())

def load_to_bigquery(project_id, dataset, table, df: pd.DataFrame):
    """Load DataFrame to BigQuery."""
    client = bigquery.Client(project_id)
    target_table = f"{project_id}.{dataset}.{table}"

    job = client.load_table_from_dataframe(
        df,
        target_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    )
    job.result()
```

**Best practices:**
- Always include a docstring explaining the script's purpose
- Use ArgumentParser for configuration (project, dataset, table)
- Use `write_disposition="WRITE_TRUNCATE"` for full refresh
- Use `write_disposition="WRITE_APPEND"` for incremental loads
- Handle errors appropriately with try/except blocks
- Use logging for important operations
- Clean up old tables before re-run if needed: `client.delete_table(table, not_found_ok=True)`

**Reference examples:**
- INFORMATION_SCHEMA query: `sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_table_storage_v1/query.py`
- External API: `sql/moz-fx-data-shared-prod/bigeye_derived/user_service_v1/query.py`

### metadata.yaml

Metadata files define table ownership, scheduling, partitioning, and other configuration.

**Required fields:**
- `friendly_name` - Human-readable table name
- `description` - Multi-line description of the table's purpose (use `>` or `|-` for multi-line)
- `owners` - List of email addresses responsible for the table

**Common labels:**
- `application` - Product/application name (e.g., `firefox`, `relay`, `mozilla_vpn`)
- `schedule` - Update frequency (e.g., `daily`, `hourly`, `weekly`)
- `table_type` - Data granularity (e.g., `client_level`, `aggregated`)
- `dag` - Airflow DAG name (e.g., `bqetl_main_summary`)
- `owner1` - Primary owner username (without @mozilla.com)

**Scheduling configuration:**
```yaml
scheduling:
  dag_name: bqetl_main_summary  # Airflow DAG to run this query
  date_partition_parameter: submission_date  # Parameter name for date (or null for full table)
  date_partition_offset: -7  # Optional: delay processing by N days
  start_date: '2021-01-19'  # Optional: when query scheduling began
  priority: 85  # Optional: Airflow task priority
```

**BigQuery configuration:**
```yaml
bigquery:
  time_partitioning:
    type: day  # or hour
    field: submission_date  # partition field name
    require_partition_filter: true  # require WHERE clause on partition
    expiration_days: 775  # optional: auto-delete old partitions
  clustering:
    fields:
      - sample_id  # first clustering field
      - client_id  # additional clustering fields
```

**Dependencies:**
```yaml
references:
  view.sql:
    - moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6
```

**Complete example (simple):**
```yaml
friendly_name: Mozilla VPN Users
description: >
  A subset of Mozilla VPN users columns that are accessible to a
  broader audience.
owners:
  - srose@mozilla.com
labels:
  application: mozilla_vpn
  schedule: daily
scheduling:
  dag_name: bqetl_subplat
  date_partition_parameter: null  # full table refresh, not partitioned query
```

**Complete example (partitioned):**
```yaml
friendly_name: Clients Daily Event
description: |-
  Daily per-client aggregations based on desktop event pings.
owners:
  - ascholtz@mozilla.com
labels:
  application: firefox
  schedule: daily
  table_type: client_level
  dag: bqetl_main_summary
  owner1: ascholtz
scheduling:
  dag_name: bqetl_main_summary
  start_date: '2021-01-19'
  priority: 85
bigquery:
  time_partitioning:
    type: day
    field: submission_date
    require_partition_filter: true
    expiration_days: 775
  clustering:
    fields:
      - sample_id
references: {}
```

**Reference examples:**
- Simple: `sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/metadata.yaml`
- Partitioned: `sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_event_v1/metadata.yaml`

### schema.yaml

Schema files define the BigQuery table structure with field names, types, modes, and descriptions.

**Field structure:**
```yaml
fields:
  - name: field_name
    type: STRING
    mode: NULLABLE
    description: Required field description explaining purpose, units, or constraints
```

**Common types:**
- `STRING` - Text data
- `INTEGER` or `INT64` - Whole numbers
- `FLOAT` or `FLOAT64` - Decimal numbers
- `BOOLEAN` - True/false values
- `DATE` - Date without time (YYYY-MM-DD)
- `TIMESTAMP` - Date and time with timezone
- `RECORD` - Nested structure (requires `fields:` sub-list)
- `NUMERIC` - High-precision decimal
- `BYTES` - Binary data

**Mode options:**
- `NULLABLE` - Field can be null (most common)
- `REQUIRED` - Field must have a value
- `REPEATED` - Array of values (for arrays/lists)

**Field ordering:**
- List fields in the order they appear in the query SELECT statement
- For consistency, order as: `name`, `type`, `mode`, then optional `description` and nested `fields`

**Nested/repeated field patterns:**

Simple repeated field:
```yaml
- name: tags
  type: STRING
  mode: REPEATED
```

Record (struct) with nested fields:
```yaml
- name: experiments
  type: RECORD
  mode: REPEATED
  fields:
    - name: key
      type: STRING
      mode: NULLABLE
    - name: value
      type: RECORD
      mode: NULLABLE
      fields:
        - name: branch
          type: STRING
          mode: NULLABLE
        - name: enrollment_id
          type: STRING
          mode: NULLABLE
```

**Simple example:**
```yaml
fields:
  - mode: NULLABLE
    name: id
    type: INTEGER
  - mode: NULLABLE
    name: fxa_uid
    type: STRING
  - mode: NULLABLE
    name: created_at
    type: TIMESTAMP
```

**Example with common telemetry fields:**
```yaml
fields:
  - name: submission_date
    type: DATE
    mode: NULLABLE
    description: Date when the data was submitted
  - name: client_id
    type: STRING
    mode: NULLABLE
    description: Unique identifier for the client
  - name: sample_id
    type: INTEGER
    mode: NULLABLE
    description: Sample identifier (0-99) for sampling
  - name: normalized_channel
    type: STRING
    mode: NULLABLE
    description: Release channel (release, beta, nightly, etc.)
  - name: country
    type: STRING
    mode: NULLABLE
    description: ISO 3166-1 alpha-2 country code
  - name: app_version
    type: STRING
    mode: NULLABLE
    description: Application version string
  - name: n_total_events
    type: INTEGER
    mode: NULLABLE
    description: Total count of events
```

**Note on descriptions:**
- Descriptions are **required** for all fields in new schemas
- Use `description:` field to document purpose, units, or constraints
- Legacy tables may have `require_column_descriptions: false` in metadata.yaml
- For new tables, always include descriptions even if they seem obvious

**Reference examples:**
- Simple: `sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/schema.yaml`
- Complex with nested fields: `sql/moz-fx-data-shared-prod/telemetry_derived/event_events_v1/schema.yaml`

## Naming Conventions

### Table Names
- Use snake_case: `clients_daily_event_v1`
- Include version suffix: `_v1`, `_v2`, etc.
- Descriptive names indicating content and granularity
- Common suffixes: `_daily`, `_hourly`, `_aggregates`, `_summary`

### Field Names
- Use snake_case: `submission_date`, `client_id`, `n_total_events`
- Prefix counts with `n_`: `n_events`, `n_sessions`
- Use common Mozilla field names:
  - `submission_date` - Date of data submission (partition field)
  - `client_id` - Unique client identifier
  - `sample_id` - Sample identifier (0-99)
  - `normalized_channel` - Release channel (release, beta, nightly)
  - `normalized_country_code` or `country` - Country code
  - `app_version` - Application version

### CTE Names
- Use descriptive names: `base`, `filtered_events`, `daily_aggregates`, `final`
- Common pattern: `base` for initial data selection
- Name by purpose, not generic names like `temp1`, `temp2`

### Reserved/Common Patterns
- Avoid `date` alone (use `submission_date`, `event_date`, etc.)
- `_derived` suffix for datasets, not individual tables
- `_external` for datasets with externally synced data
- UDF naming: `mozfun.category.function_name` (e.g., `mozfun.map.get_key`)

## BigQuery & Mozilla Conventions

### Standard Project Names
- Production: `moz-fx-data-shared-prod` (primary data warehouse)
- Other environments: `moz-fx-{product}-{environment}`
- Functions library: `mozfun` (UDFs accessible across projects)

### Partitioning Strategies
- **Day partitioning** (most common):
  - Field: `submission_date` (DATE type)
  - Use `require_partition_filter: true` for large tables
  - Query with: `WHERE submission_date = @submission_date`

- **Timestamp partitioning**:
  - Field: `submission_timestamp` (TIMESTAMP type)
  - Use when hour-level partitioning needed

- **Expiration**:
  - Set `expiration_days: 775` (approx 2 years + buffer) for client-level data
  - Adjust based on data retention requirements

### Clustering Conventions
- Order by query pattern frequency (most filtered/joined first)
- Common patterns:
  - Client-level: `[sample_id, client_id]`
  - Event-level: `[event_category, sample_id, client_id]`
  - Time-series: `[submission_date, sample_id]`
- Max 4 clustering fields

### Data Retention
- Client-level telemetry: ~2 years (775 days)
- Aggregated data: Often longer or no expiration
- Check organizational policies for specific data types

### Privacy & Data Sensitivity
- Avoid PII in derived tables
- Use `sample_id` for sampling (0-99, represents 1% sample when = 0)
- Client-level tables should use `table_type: client_level` label
- Reference Mozilla data policies for handling sensitive fields

### Common UDFs (mozfun)
- `mozfun.map.get_key(map, key)` - Extract value from key-value array
- `mozfun.norm.truncate_version(version, 'major')` - Parse version strings
- `mozfun.stats.mode_last(array)` - Get most recent mode from array
- Browse `sql/mozfun/` for available functions

### Privacy & Data Handling

**Key principles from Mozilla's data platform:**
- **Geo IP lookup**: IP addresses are discarded after geo-city metadata extraction
- **User agent parsing**: Raw user agent strings are discarded after feature extraction
- **Deduplication**: Best-effort deduplication in Pub/Sub, full deduplication per UTC day in BigQuery
- **Deletion requests**: Support for user deletion requests via `deletion-request` pings
- **Sample ID**: Use `sample_id` field (0-99) for sampling, enables 1% samples when `sample_id = 0`

**Best practices:**
- Don't include PII (personally identifiable information) in derived tables
- Use client-level identifiers (`client_id`) not individual-level identifiers
- Respect data retention policies (typically ~2 years for client-level data)
- Label client-level tables with `table_type: client_level` in metadata.yaml
- Redact or aggregate data that could identify individuals
- Follow structured ingestion for automatic geo/user-agent anonymization

**For detailed privacy guidelines:**
- See: https://docs.telemetry.mozilla.org/tools/guiding_principles.html
- Contact: Data Platform team for privacy review questions

## Hourly and Sub-Daily Queries

For queries that run more frequently than daily (e.g., hourly):

**Key differences from daily queries:**

1. **Set `date_partition_parameter: null`** in metadata.yaml scheduling section
2. **Use explicit `destination_table`** with partition decorator in Jinja:
   ```yaml
   destination_table: >-
     table_name_v1${{
     (execution_date - macros.timedelta(hours=1)).strftime("%Y%m%d")
     }}
   ```
3. **Use `submission_date` parameter** (still date-based, not timestamp):
   ```yaml
   parameters:
     - >-
       submission_date:DATE:{{
       (execution_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d')
       }}
   ```
4. **Query still filters by date**, not hour:
   ```sql
   WHERE DATE(submission_timestamp) = @submission_date
   ```
5. **Reprocess same partition multiple times** - Hourly runs overwrite the same day's partition until complete

**Why this pattern:**
- Hourly jobs process the previous hour's data
- All data for a given day goes into the same partition
- Each hourly run overwrites the partition with cumulative day's data
- Ensures late-arriving data from live tables is included

**Reference example:**
- `sql/moz-fx-data-shared-prod/telemetry_derived/newtab_interactions_hourly_v1/`
  - Shows hourly DAG with explicit destination_table
  - Demonstrates using `submission_date` parameter for date-partitioned hourly queries
  - Uses `_live` tables as source for low-latency data

## Workflow: From Requirements to Files

When given business requirements:

1. **Understand the requirement**
   - Identify source tables
   - Determine transformations needed
   - Clarify aggregation/grouping logic

2. **Plan the structure**
   - Choose appropriate project/dataset
   - Determine table name and version
   - Identify dependencies

3. **Generate query.sql**
   - Write SQL following conventions
   - Include appropriate comments
   - Use CTEs for clarity
   - Apply partitioning/clustering

4. **Create metadata.yaml**
   - Set owners and scheduling
   - Declare dependencies
   - Add appropriate labels

5. **Define schema.yaml**
   - List all output fields
   - Specify types and modes
   - Write clear descriptions

## bigquery-etl Documentation

Internal documentation available in `docs/`:
- **Creating derived datasets**: `docs/cookbooks/creating_a_derived_dataset.md`
  - Complete guide to creating tables with `./bqetl query create`
  - Scheduling with Airflow DAGs
  - Backfilling and deployment process
- **Recommended practices**: `docs/reference/recommended_practices.md`
  - Query best practices (naming, formatting, optimization)
  - Metadata structure and labels
  - UDF guidelines
- **Incremental queries**: `docs/reference/incremental.md`
  - Benefits of incremental queries
  - Required properties (`@submission_date` parameter)
- **Common workflows**: `docs/cookbooks/common_workflows.md`
- **Testing**: `docs/cookbooks/testing.md`

**Key bqetl CLI commands:**
- `./bqetl query create <dataset>.<table> --dag <dag_name>` - Create new query with templates
- `./bqetl query validate <dataset>.<table>` - Validate query syntax
- `./bqetl query schema update <dataset>.<table>` - Generate schema.yaml from query
- `./bqetl query schema deploy <dataset>.<table>` - Deploy schema to BigQuery
- `./bqetl dag create <dag_name>` - Create new Airflow DAG
- `./bqetl backfill create <dataset>.<table> --start_date=<date> --end_date=<date>` - Create backfill

## Reference Repositories

### data-docs Repository

Location: `../data-docs/`

Contains documentation for:
- Dataset descriptions: `../data-docs/src/datasets/`
- BigQuery cookbooks: `../data-docs/src/cookbooks/bigquery/`
- Accessing desktop data guide: `../data-docs/src/cookbooks/bigquery/accessing_desktop_data.md`
- Common datasets like `clients_daily`, `clients_last_seen`, etc.
- Glean overview: `../data-docs/src/concepts/glean/glean.md`

**When to reference:**
- Understanding what data is available in a dataset
- Finding examples of how to query specific data sources
- Learning about raw pings vs derived datasets

### glean-dictionary Repository

Location: `../glean-dictionary/`
Production URL: https://dictionary.telemetry.mozilla.org

The Glean Dictionary provides a comprehensive index of datasets generated by applications built using the Glean SDKs.

**What is Glean:**
- Mozilla's product analytics & telemetry solution
- Provides consistent measurement across all Mozilla products
- Different from Firefox Desktop Telemetry (legacy system)
- Uses metric types and pings to send structured data
- Enforces data review and expiration dates for all metrics
- Auto-generates documentation for metrics

**Key concepts:**
- **Glean SDK**: Performs measurements and sends data from products
- **Metric types**: Predefined measurement types (counter, boolean, string, event, etc.)
- **Pings**: Collections of metrics sent over the network (e.g., `baseline`, `events`, `metrics`)
- **Applications**: Products using Glean (e.g., Fenix, Focus, Firefox iOS)

**Common Glean datasets in BigQuery:**
- Pattern: `{app_id}.{ping_name}` (e.g., `org_mozilla_fenix.baseline`, `org_mozilla_fenix.events`)
- Common pings: `baseline`, `events`, `metrics`, `deletion_request`
- All have automatically generated schemas based on metric definitions

**When to reference:**
- Working with Glean-based applications (Fenix, Focus, Firefox iOS, etc.)
- Understanding available metrics for a Glean application
- Looking up metric definitions, types, and expiration dates
- Finding which pings contain specific metrics
- Understanding Glean dataset structures

**ETL directory:**
- `../glean-dictionary/etl/` - Contains Python code for building metadata
- Can be useful for understanding how Glean metadata is processed

## Common Patterns

### Standard Date Partitioning

All incremental queries should filter on the partition field:

```sql
WHERE
  submission_date = @submission_date
```

For queries with time ranges:
```sql
{% if is_init() %}
  -- Full historical backfill
  AND submission_date BETWEEN '2020-01-01' AND @submission_date
{% else %}
  -- Incremental single partition
  AND submission_date = @submission_date
{% endif %}
```

### User-Level Aggregations

Common pattern for aggregating by client:

```sql
WITH base AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    country,
    -- Your fields here
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = @submission_date
)
SELECT
  submission_date,
  client_id,
  sample_id,
  COUNT(*) AS n_events,
  SUM(metric_value) AS total_metric,
  MAX(last_seen) AS last_activity,
FROM
  base
GROUP BY
  submission_date,
  client_id,
  sample_id
```

Always include `client_id` and `submission_date` in GROUP BY for client-level tables.

### Event Processing with Bit Patterns

For tracking activity over time periods (e.g., 28 days), use bit patterns:

```sql
-- Track if user was active today
CAST(active_hours_sum > 0 AS INT64) AS days_active_bits,

-- Track URI visits with thresholds
CAST(uri_count >= 5 AS INT64) AS days_visited_5_uri_bits,
```

Bit patterns allow efficient historical tracking in single integer fields.

### Common JOINs

**LEFT JOIN to enrich with client metadata:**
```sql
SELECT
  events.*,
  clients.country,
  clients.channel,
  clients.os
FROM
  `moz-fx-data-shared-prod.telemetry.events` AS events
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.clients_daily` AS clients
  ON events.client_id = clients.client_id
  AND events.submission_date = clients.submission_date
WHERE
  events.submission_date = @submission_date
```

**Always join on both `client_id` AND `submission_date`** for partitioned tables to optimize performance.

### Standard Metrics Calculations

**Active users:**
```sql
COALESCE(
  scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
  scalar_parent_browser_engagement_total_uri_count_sum
) > 0
```

**Using mozfun UDFs:**
```sql
-- Truncate version to major
mozfun.norm.truncate_version(app_version, 'major')

-- Extract from key-value maps
mozfun.map.get_key(event_map_values, 'key_name')

-- Safe timestamp conversion
mozfun.norm.extract_version(version_string, 'major')
```

### UNNEST Patterns for Events

```sql
SELECT
  submission_date,
  client_id,
  event.timestamp AS event_timestamp,
  event.category AS event_category,
  event.method AS event_method,
FROM
  `moz-fx-data-shared-prod.telemetry.event`
CROSS JOIN
  UNNEST(payload.events.parent) AS event
WHERE
  submission_date = @submission_date
```

## Examples

Reference examples are provided throughout this document:
- **Simple query**: `sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/query.sql`
- **Aggregation with GROUP BY**: `sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_event_v1/query.sql`
- **Complex query with CTEs**: `sql/moz-fx-data-shared-prod/telemetry_derived/event_events_v1/query.sql`
- **Python ETL (INFORMATION_SCHEMA)**: `sql/moz-fx-data-shared-prod/monitoring_derived/bigquery_table_storage_v1/query.py`
- **Python ETL (External API)**: `sql/moz-fx-data-shared-prod/bigeye_derived/user_service_v1/query.py`
- **Metadata examples**: See metadata.yaml sections above
- **Schema examples**: See schema.yaml sections above

For more examples, explore the `sql/moz-fx-data-shared-prod/` directory.

## Best Practices

**Performance optimization:**
- Filter on partition and clustering columns early in queries
- Use `require_partition_filter: true` for large tables
- Avoid `SELECT *` - explicitly list needed columns
- Use clustering for frequently filtered columns
- See: [docs/reference/recommended_practices.md](docs/reference/recommended_practices.md)
- See: https://docs.telemetry.mozilla.org/cookbooks/bigquery/optimization.html

**Testing:**
- Use `./bqetl query validate` to check SQL syntax
- Test queries in sql.telemetry.mozilla.org before deployment
- Validate schema matches query output with `./bqetl query schema update`
- See: [docs/cookbooks/testing.md](docs/cookbooks/testing.md)

**Documentation:**
- Always include field descriptions in schema.yaml
- Add header comments explaining query purpose
- Reference bug/ticket numbers for context
- Document any data exclusions or filtering logic

**Version migration:**
- Create new `_v2` table when making breaking schema changes
- Keep `_v1` running during migration period
- Update views to point to new version
- Coordinate with downstream consumers before deprecating old version

## Questions to Ask

If requirements are unclear, ask about:
- Data sources and their update frequency
- Granularity needed (user-level, daily aggregates, etc.)
- Partitioning requirements
- Expected data volume
- Downstream consumers
- Privacy/sensitivity classification
