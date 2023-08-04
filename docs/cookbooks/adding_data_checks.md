# Adding data checks

> Before adding a check to a query, please remember that the current implementation will result in an Airflow task being added to the DAG which will be executed immediately after the query task completes. If any of the defined checks fail, all downstream tasks from the query task will fail (`checks` act as a "circuit-breaker"). This means all downstream tasks and DAGs will not be executed!

## Create checks.sql

Inside the query directory, which usually contains `query.sql` or `query.py`, `metadata.yaml` and `schema.yaml`, create a new file called `checks.sql` (unless already exists).

Once checks have been added, we need to `regenerate the DAG` responsible for scheduling the query.

## Update checks.sql

If `checks.sql` already exists for the query, you can always add additional checks to the file by appending it to the list of already defined checks.

When adding additional checks there should be no need to have to regenerate the DAG responsible for scheduling the query as all checks are executed using a single Airflow task.

## Removing checks.sql

All checks can be removed by deleleting the `checks.sql` file and regenerating the DAG responsible for scheduling the query.

Alternatively, specific checks can be removed by deleting them from the `checks.sql` file.

## Example checks.sql

Example of what a `checks.sql` may look like:

```sql
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}
 {{ min_rows(1, "submission_date = @submission_date") }}
 {{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}
 {{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
```