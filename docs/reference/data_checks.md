# bqetl Data Checks

> Instructions on how to add data checks can be found in the [Adding data checks](#adding-data-checks) section below.

## Background

To create more confidence and trust in our data is crucial to provide some form of data checks. These checks should uncover problems as soon as possible, ideally as part of the data process creating the data. This includes checking that the data produced follows certain assumptions determined by the dataset owner. These assumptions need to be easy to define, but at the same time flexible enough to encode more complex business logic. For example, checks for null columns, for range/size properties, duplicates, table grain etc.

## bqetl Data Checks to the Rescue

bqetl data checks aim to provide this ability by providing a simple interface for specifying our "assumptions" about the data the query should produce and checking them against the actual result.

This easy interface is achieved by providing a number of jinja templates providing "out-of-the-box" logic for performing a number of common checks without having to rewrite the logic. For example, checking if any nulls are present in a specific column. These templates can be found [here](../../tests/checks/) and are available as jinja macros inside the `checks.sql` files. This allows to "configure" the logic by passing some details relevant to our specific dataset. Check templates will get rendered as raw SQL expressions. Take a look at the examples below for practical examples.

It is also possible to write checks using raw SQL by using assertions. This is, for example, useful when writing checks for custom business logic.

### Two categories of checks

Each check needs to be categorised with a marker, currently following markers are available:

- `#fail` indicates that the ETL pipeline should stop if this check fails (circuit-breaker pattern) and a notification is sent out. _This marker should be used for checks that indicate a serious data issue._

- `#warn` indicates that the ETL pipeline should continue even if this check fails. These type of checks can be used to indicate _potential_ issues that might require more manual investigation.

Checks can be marked by including one of the markers on the line preceeding the check definition, see _Example checks.sql_ section for an example.

## Adding Data Checks

### Create checks.sql

Inside the query directory, which usually contains `query.sql` or `query.py`, `metadata.yaml` and `schema.yaml`, create a new file called `checks.sql` (unless already exists).

> Please make sure each check you add contains a marker (see: the _Two categories of checks_ section above).

Once checks have been added, we need to `regenerate the DAG` responsible for scheduling the query.

### Update checks.sql

If `checks.sql` already exists for the query, you can always add additional checks to the file by appending it to the list of already defined checks.

When adding additional checks there should be no need to have to regenerate the DAG responsible for scheduling the query as all checks are executed using a single Airflow task.

### Removing checks.sql

All checks can be removed by deleting the `checks.sql` file and regenerating the DAG responsible for scheduling the query.

Alternatively, specific checks can be removed by deleting them from the `checks.sql` file.

### Example checks.sql

Checks can either be written as raw SQL, or by referencing existing Jinja macros defined in [`tests/checks`](https://github.com/mozilla/bigquery-etl/tree/main/tests/checks) which may take different parameters used to generate the SQL check expression.

Example of what a `checks.sql` may look like:

```sql
-- raw SQL checks
#fail
ASSERT (
  SELECT
    COUNTIF(ISNULL(country)) / COUNT(*)
    FROM telemetry.table_v1
    WHERE submission_date = @submission_date
  ) > 0.2
) AS "More than 20% of clients have country set to NULL";

-- macro checks
#fail
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}

#warn
{{ min_row_count(1, "submission_date = @submission_date") }}

#fail
{{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}

#warn
{{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
```

## Data Checks Available with Examples

### accepted_values ([source](../../tests/accepted_values.jinja))

Usage:
```
Arguments:

column: str - name of the column to check
values: List[str] - list of accepted values
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
#warn
{{ accepted_values("column_1", ["value_1", "value_2"],"submission_date = @submission_date") }}
```

### in_range ([source](../../tests/checks/in_range.jinja))

Usage:

```
Arguments:

columns: List[str] - A list of columns which we want to check the values of.
min: Optional[int] - Minimum value we should observe in the specified columns.
max: Optional[int] - Maximum value we should observe in the specified columns.
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
#warn
{{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
```

### is_unique ([source](../../tests/checks/is_unique.jinja))

Usage:

```
Arguments:

columns: List[str] - A list of columns which should produce a unique record.
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
#warn
{{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}
```

### min_row_count([source](../../tests/checks/min_row_count.jinja))

Usage:

```
Arguments:

threshold: Optional[int] - What is the minimum number of rows we expect (default: 1)
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
#fail
{{ min_row_count(1, "submission_date = @submission_date") }}
```

### not_null ([source](../../tests/checks/not_null.jinja))

Usage:

```
Arguments:

columns: List[str] - A list of columns which should not contain a null value.
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
#fail
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}
```

Please keep in mind the below checks can be combined and specified in the same `checks.sql` file. For example:

```sql
#fail
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}
 #fail
 {{ min_row_count(1, "submission_date = @submission_date") }}
 #fail
 {{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}
 #warn
 {{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
```

### row_count_within_past_partitions_avg([source](../../tests/checks/row_count_within_past_partitions_avg.jinja))

Compares the row count of the current partition to the average of `number_of_days` past partitions and checks if the row count is within the average +- `threshold_percentage` %

Usage:

```
Arguments:

number_of_days: int - Number of days we are comparing the row count to
threshold_percentage: int - How many percent above or below the average row count is ok.
partition_field: Optional[str] - What column is the partition_field (default = "submission_date")
```

Example:
```sql
#fail
{{ row_count_within_past_partitions_avg(7, 5, "submission_date") }}
```

### value_length([source](../../tests/checks/value_length.jinja))

Checks that the column has values of specific character length.

Usage:

```
Arguments:

column: str - Column which will be checked against the `expected_length`.
expected_length: int - Describes the expected character length of the value inside the specified columns.
where: Optional[str]: Any additional filtering rules that should be applied when retrieving the data to run the check against.
```

Example:
```sql
#warn
{{ value_length(column="country", expected_length=2, where="submission_date = @submission_date") }}
```

### matches_pattern([source](../../tests/checks/matches_pattern.jinja))

Checks that the column values adhere to a pattern based on a regex expression.

Usage:

```
Arguments:

column: str - Column which values will be checked against the regex.
pattern: str - Regex pattern specifying the expected shape / pattern of the values inside the column.
where: Optional[str]: Any additional filtering rules that should be applied when retrieving the data to run the check against.
threshold_fail_percentage: Optional[int] - Percentage of how many rows can fail the check before causing it to fail.
message: Optional[str]: Custom error message.
```

Example:
```sql
#warn
{{ matches_pattern(column="country", pattern="^[A-Z]{2}$", where="submission_date = @submission_date", threshold_fail_percentage=10, message="Oops") }}
```


## Running checks locally / Commands

To list all available commands in the bqetl data checks CLI:

```shell
$ ./bqetl check

Usage: bqetl check [OPTIONS] COMMAND [ARGS]...

  Commands for managing and running bqetl data checks.

  ––––––––––––––––––––––––––––––––––––––––––––––

  IN ACTIVE DEVELOPMENT

  The current progress can be found under:

          https://mozilla-hub.atlassian.net/browse/DENG-919

  ––––––––––––––––––––––––––––––––––––––––––––––

Options:
  --help  Show this message and exit.

Commands:
  render  Renders data check query using parameters provided (OPTIONAL).
  run     Runs data checks defined for the dataset (checks.sql).
```

To see see how to use a specific command use:

```shell
$ ./bqetl check [command] --help
```

---

`render`

### Usage

```shell
$ ./bqetl check render [OPTIONS] DATASET [ARGS]

Renders data check query using parameters provided (OPTIONAL). The result
is what would be used to run a check to ensure that the specified dataset
adheres to the assumptions defined in the corresponding checks.sql file

Options:
  --project-id, --project_id TEXT
                                  GCP project ID
  --sql_dir, --sql-dir DIRECTORY  Path to directory which contains queries.
  --help                          Show this message and exit.
```

### Example

```shell
./bqetl check render --project_id=moz-fx-data-marketing-prod ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01
```

---

`run`

### Usage

```shell
$ ./bqetl check run [OPTIONS] DATASET

Runs data checks defined for the dataset (checks.sql).

Checks can be validated using the `--dry_run` flag without executing them:

Options:
  --project-id, --project_id TEXT
                                  GCP project ID
  --sql_dir, --sql-dir DIRECTORY  Path to directory which contains queries.
  --dry_run, --dry-run            To dry run the query to make sure it is
                                  valid
  --marker TEXT                   Marker to filter checks.
  --help                          Show this message and exit.
```

### Examples

```shell
# to run checks for a specific dataset
$ ./bqetl check run ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01 --marker=fail --marker=warn

# to only dry_run the checks
$ ./bqetl check run --dry_run ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01 --marker=fail
```
