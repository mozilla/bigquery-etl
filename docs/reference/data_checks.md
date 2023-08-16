# bqetl data checks

> Instructions on how to add data checks can be found under the [Adding data checks](../cookbooks/adding_data_checks.md) cookbook.

## Background

To create more confidence and trust in our data is crucial to provide some form of data checks. These checks should uncover problems as soon as possible, ideally as part of the data process creating the data. This includes checking that the data produced follows certain assumptions determined by the dataset owner. These assumptions need to be easy to define, but at the same time flexible enough to encode more complex business logic. For example, checks for null columns, for range/size properties, duplicates, table grain etc.

## bqetl data checks to the rescue

bqetl data checks aim to provide this ability by providing a simple interface for specifying our "assumptions" about the data the query should produce and checking them against the actual result.

This easy interface is achieved by providing a number of jinja templates providing "out-of-the-box" logic for performing a number of common checks without having to rewrite the logic. For example, checking if any nulls are present in a specific column. These templates can be found [here](../../tests/checks/) and are available as jinja macros inside the `checks.sql` files. This allows to "configure" the logic by passing some details relevant to our specific dataset. Check templates will get rendered as raw SQL expressions. Take a look at the examples below for practical examples.

It is also possible to write checks using raw SQL by using assertions. This is, for example, useful when writing checks for custom business logic.

## data checks available with examples

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
{{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}
```

### min_rows ([source](../../tests/checks/min_rows.jinja))

Usage:

```
Arguments:

threshold: Optional[int] - What is the minimum number of rows we expect (default: 1)
where: Optional[str] - A condition that will be injected into the `WHERE` clause of the check. For example, "submission_date = @submission_date" so that the check is only executed against a specific partition.
```

Example:

```sql
{{ min_rows(1, "submission_date = @submission_date") }}
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
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}
```

Please keep in mind the below checks can be combined and specified in the same `checks.sql` file. For example:

```sql
{{ not_null(["submission_date", "os"], "submission_date = @submission_date") }}
 {{ min_rows(1, "submission_date = @submission_date") }}
 {{ is_unique(["submission_date", "os", "country"], "submission_date = @submission_date")}}
 {{ in_range(["non_ssl_loads", "ssl_loads", "reporting_ratio"], 0, none, "submission_date = @submission_date") }}
```
