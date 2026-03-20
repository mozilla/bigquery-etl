# Schema includes

The [YAML tags](#yaml-tags) listed below can be used in `schema.yaml` files to include content from:
- stable table schemas (i.e. telemetry tables in `*_stable` datasets)
- `schema.yaml` files for tables or views
- arbitrary YAML files (e.g. [`bigquery_etl/schema/global.yaml`](https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/schema/global.yaml))

Some recommendations when using this feature:
- Be mindful to arrange the includes so the resulting rendered `schema.yaml` file matches the actual schema of the query when it runs.
- Have the includes mimic the structure of the associated query:
  - If the query passes through all fields from an upstream table, then use `!include-fields` with no field parameter.
  - If the query selects specific fields from an upstream table, then use `!include-fields` with the `field_names` parameter specifying fields in the same order the query selects them.
  - If the query selects most fields from an upstream table with `SELECT * EXCEPT (...)`, then use `!include-fields` with the `exclude_field_names` parameter.
  - If the query replaces some fields from an upstream table with `SELECT * REPLACE (...)`, then use `!include-fields` with the `field_replacements` parameter.
- Avoid using the `bqetl query schema update` command on `schema.yaml` files with includes, as that will overwrite the includes.

## YAML tags

### `!include-field`

Includes a field from the specified table/view or schema YAML file.

#### Parameters:
- Either `table` or `file` is required:
  - `table`: Fully qualified ID of the table/view to include from (must have a `schema.yaml` file or be a stable table).
  - `file`: File path of the schema YAML file to include from (relative to the root of the repository).
- `field`: Field path of the field to include.
- `append_description`: Optional text to append to the included field's description.
- `prepend_description`: Optional text to prepend to the included field's description.

#### Examples:
```yaml
# Include a top-level column from an ETL table.
fields:
- !include-field
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
  field: default_search_engine
```
```yaml
# Include a top-level column from an ETL table, and append extra text to the description.
fields:
- !include-field
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
  field: default_search_engine
  append_description: And one more thing...
```
```yaml
# Include a nested field from a stable table.
fields:
- !include-field
  table: moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1
  field: client_info.client_id
```
```yaml
# Include a field from a YAML file.
fields:
- !include-field
  file: /bigquery_etl/schema/global.yaml
  field: app_build_id
```

### `!include-fields`

Includes fields from the specified table/view, schema YAML file, or struct field in the table/view/file.

If the included fields are being inserted into part of a larger list, then the [`!flatten-lists`](#flatten-lists) tag will also need to be used (see examples below).

#### Parameters:
- Either `table` or `file` is required:
  - `table`: Fully qualified ID of the table/view to include from (must have a `schema.yaml` file or be a stable table).
  - `file`: File path of the schema YAML file to include from (relative to the root of the repository).
- `parent_field`: Optional field path of the struct parent field to include fields from.
- `field_names`: Optional list of fields to include (either top-level columns, or nested fields if `parent_field` is specified).
- `exclude_field_names`: Optional list of fields to exclude (either top-level columns, or nested fields if `parent_field` is specified).
- `field_replacements`: Optional list of field definitions that will be used in place of the associated field definitions found in the include (matched by field name).

#### Examples:
```yaml
# Include all top-level columns from an ETL table.
fields: !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
```
```yaml
# Include all top-level columns from an ETL table alongside additional fields.
# Note the !flatten-lists tag to flatten the nested list created by using !include-fields in a list item.
fields: !flatten-lists
- name: first_seen_date
  type: DATE
  mode: NULLABLE
  description: The date when the telemetry ping was first received on the server side.
- !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
```
```yaml
# Include most top-level columns from an ETL table.
fields: !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
  exclude_field_names:
  - client_id
  - sample_id
```
```yaml
# Include specific top-level columns from an ETL table.
fields: !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
  field_names:
  - submission_date
  - normalized_channel
```
```yaml
# Include all top-level columns from an ETL table, but replace a string field with a JSON version.
fields: !include-fields
  table: moz-fx-data-shared-prod.stripe_external.product_v1
  field_replacements:
  - name: metadata
    type: JSON
    mode: NULLABLE
    description: Set of key-value pairs attached to the product, stored as a JSON object.
```
```yaml
# Include all nested fields in a struct from a stable table.
fields: !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1
  parent_field: client_info
```
```yaml
# Include specific nested fields in a struct from a stable table.
fields: !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1
  parent_field: client_info
  field_names:
  - app_build
  - app_channel
```
```yaml
# Include specific fields from a YAML file.
fields: !include-fields
  file: /bigquery_etl/schema/global.yaml
  field_names:
  - app_build_id
  - app_name
```

### `!include-field-description`

Includes a field description from the specified table/view or schema YAML file.

#### Parameters:
- Either `table` or `file` is required:
  - `table`: Fully qualified ID of the table/view to include from (must have a `schema.yaml` file or be a stable table).
  - `file`: File path of the schema YAML file to include from (relative to the root of the repository).
- `field`: Field path of the field to include the description from.
- `append`: Optional text to append to the description.
- `prepend`: Optional text to prepend to the description.

#### Examples:
```yaml
# Include a top-level column description from an ETL table.
fields:
- name: channel
  type: STRING
  mode: NULLABLE
  description: !include-field-description
    table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
    field: normalized_channel
```
```yaml
# Include a top-level column description from an ETL table, and append extra text to the description.
fields:
- name: channel
  type: STRING
  mode: NULLABLE
  description: !include-field-description
    table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
    field: normalized_channel
    append: And one more thing...
```
```yaml
# Include a nested field description from a stable table.
fields:
- name: client_locale
  type: STRING
  mode: NULLABLE
  description: !include-field-description
    table: moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1
    field: client_info.locale
```
```yaml
# Include a field description from a YAML file.
fields:
- name: version
  type: STRING
  mode: NULLABLE
  description: !include-field-description
    file: /bigquery_etl/schema/global.yaml
    field: app_version
```

### `!include`

Includes data from a YAML file.

#### Parameters:
- `file`: File path of the YAML file to include from (relative to the root of the repository).
- `jmespath`: Optional [JMESPath](https://jmespath.org/) expression to select the data.

#### Examples:
```yaml
# Include an entire YAML file.
!include
file: /sql/moz-fx-data-shared-prod/firefox_desktop_derived/metrics_clients_daily_v1/schema.yaml
```
```yaml
# Include a specific field from a `schema.yaml` file.
fields:
- !include
  file: /sql/moz-fx-data-shared-prod/firefox_desktop_derived/metrics_clients_daily_v1/schema.yaml
  jmespath: fields[?name == 'default_search_engine'] | [0]
```
```yaml
# Include a specific subset of fields from a `schema.yaml` file.
fields: !include
  file: /sql/moz-fx-data-shared-prod/firefox_desktop_derived/metrics_clients_daily_v1/schema.yaml
  jmespath: fields[?contains(['submission_date', 'normalized_channel'], name)]
```

### `!flatten-lists`

When applied to a list it will flatten any directly nested lists, like those created by using [`!include-fields`](#include-fields) in a list item.

#### Examples:
```yaml
# Include all top-level columns from an ETL table alongside additional fields.
fields: !flatten-lists
- name: first_seen_date
  type: DATE
  mode: NULLABLE
  description: The date when the telemetry ping was first received on the server side.
- !include-fields
  table: moz-fx-data-shared-prod.firefox_desktop_derived.metrics_clients_daily_v1
```

## Rendering `schema.yaml` files with includes

The [`bqetl query schema render`](https://mozilla.github.io/bigquery-etl/bqetl/#render_1) command can render `schema.yaml` files for queries and views, resolving all includes, and printing to the console by default.

The [GitHub Actions jobs](https://github.com/mozilla/bigquery-etl/blob/main/.github/workflows/build.yml) use the `bqetl query schema render` command to resolve all `schema.yaml` includes before doing diffs or publishing files to the [`generated-sql`](https://github.com/mozilla/bigquery-etl/tree/generated-sql) branch.
