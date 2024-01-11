# A quick guide to creating a derived dataset with BigQuery-ETL and how to set it up as a public dataset

This guide takes you through the creation of a simple derived dataset using bigquery-etl and scheduling it using Airflow, to be updated on a daily basis. It applies to the products we ship to customers, that use (or will use) the Glean SDK.

This guide also includes the specific instructions to set it as a [public dataset](https://blog.mozilla.org/data/2020/09/25/data-publishing-mozilla/).
_Make sure you only set the dataset public if you expect the data to be available outside Mozilla_. Read our [public datasets reference](https://mozilla.github.io/bigquery-etl/reference/public_data/) for context.

To illustrate the overall process, we will use a simple test case and a small [Glean](https://mozilla.github.io/glean/) application for which we want to generate an aggregated dataset based on the raw ping data.

If you are interested in looking at the end result, you can view the pull request at [mozilla/bigquery-etl#1760](https://github.com/mozilla/bigquery-etl/pull/1760).

## Background

[Mozregression](https://mozilla.github.io/mozregression/) is a developer tool used to help developers and community members bisect builds of Firefox to find a regression range in which a bug was introduced. It forms a key part of our quality assurance process.

In this example, we will create a table of aggregated metrics related to `mozregression`, that will be used in dashboards to help prioritize feature development inside Mozilla.

## Initial steps

Set up bigquery-etl on your system per the instructions in the [README.md](https://github.com/mozilla/bigquery-etl/blob/main/README.md).

## Create the Query

The first step is to create a query file and decide on the name of your derived dataset. In this case, we'll name it `org_mozilla_mozregression_derived.mozregression_aggregates`.

The `org_mozilla_mozregression_derived` part represents a [BigQuery dataset](https://cloud.google.com/bigquery/docs/datasets-intro), which is essentially a container of tables. By convention, we use the `_derived` postfix to hold derived tables like this one.

Run:
```bash
./bqetl query create <dataset>.<table_name>
```
In our example:

```bash
./bqetl query create org_mozilla_mozregression_derived.mozregression_aggregates --dag bqetl_internal_tooling
```

This command does three things:

- Generate the template files `metadata.yaml` and `query.sql` representing the query to build the dataset in `sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1`
- Generate a "view" of the dataset in `sql/moz-fx-data-shared-prod/org_mozilla_mozregression/mozregression_aggregates`.
- Add the scheduling information in the metadata, required to create a task in Airflow DAG `bqetl_internal_tooling`.
  - When the dag name is not given, the query is scheduled by default in DAG `bqetl_default`.
  - When the option `--no-schedule` is used, queries are not schedule. This option is available for queries that run once or should be scheduled at a later time. The query can be manually scheduled at a later time.

We generate the view to have a stable interface, while allowing the dataset backend to evolve over time. Views are automatically published to the `mozdata` project.

## Fill out the YAML

The next step is to modify the generated `metadata.yaml` and `query.sql` sections with specific information.

Let's look at what the `metadata.yaml` file for our example looks like. Make sure to adapt this file for your own dataset.

```yaml
friendly_name: mozregression aggregates
description:
  Aggregated metrics of mozregression usage
labels:
  incremental: true
owners:
  - wlachance@mozilla.com
bigquery:
  time_partitioning:
    type: day
    field: date
    require_partition_filter: true
    expiration_days: null
  clustering:
    fields:
    - app_used
    - os
```

Most of the fields are self-explanatory. `incremental` means that the table is updated incrementally, e.g. a new partition gets added/updated to the destination table whenever the query is run. For non-incremental queries the entire destination is overwritten when the query is executed.

[For big datasets make sure to include optimization strategies](https://docs.telemetry.mozilla.org/cookbooks/bigquery/optimization.html). Our aggregation is small so it is only for illustration purposes that we are including a partition by the `date` field and a clustering on `app_used` and `os`.


#### The YAML file structure for a public dataset
Setting the dataset as public means that it will be both in Mozilla's public BigQuery project and a world-accessible JSON endpoint, and is a process that requires a data review.
The required labels are: `public_json`, `public_bigquery` and `review_bugs` which refers to the Bugzilla bug where opening this data set up to the public was approved: we'll get to that in a subsequent section.

```yaml
friendly_name: mozregression aggregates
description:
  Aggregated metrics of mozregression usage
labels:
  incremental: true
  public_json: true
  public_bigquery: true
  review_bugs:
    - 1691105
owners:
  - wlachance@mozilla.com
bigquery:
  time_partitioning:
    type: day
    field: date
    require_partition_filter: true
    expiration_days: null
  clustering:
    fields:
    - app_used
    - os
```

## Fill out the query

Now that we've filled out the metadata, we can look into creating a query. In many ways, this is similar to creating a SQL query to run on BigQuery in other contexts (e.g. on sql.telemetry.mozilla.org or the BigQuery console)-- the key difference is that we use a `@submission_date` parameter so that the query can be run on a _day's worth_ of data to update the underlying table incrementally.

Test your query and add it to the `query.sql` file.

In our example, the query is tested in `sql.telemetry.mozilla.org`, and the `query.sql` file looks like this:


```sql
SELECT
  DATE(submission_timestamp) AS date,
  client_info.app_display_version AS mozregression_version,
  metrics.string.usage_variant AS mozregression_variant,
  metrics.string.usage_app AS app_used,
  normalized_os AS os,
  mozfun.norm.truncate_version(normalized_os_version, "minor") AS os_version,
  count(DISTINCT(client_info.client_id)) AS distinct_clients,
  count(*) AS total_uses
FROM
  `moz-fx-data-shared-prod`.org_mozilla_mozregression.usage
WHERE
  DATE(submission_timestamp) = @submission_date
  AND client_info.app_display_version NOT LIKE '%.dev%'
GROUP BY
  date,
  mozregression_version,
  mozregression_variant,
  app_used,
  os,
  os_version;
```

We use the `truncate_version` UDF to omit the patch level for MacOS and Linux, which should both reduce the size of the dataset as well as make it more difficult to identify individual clients in an aggregated dataset.

We also have a short clause (`client_info.app_display_version NOT LIKE '%.dev%'`) to omit developer versions from the aggregates: this makes sure we're not including people developing or testing mozregression itself in our results.

## Formatting and validating the query

Now that we've written our query, we can format it and validate it. Once that's done, we run:

```bash
./bqetl query validate <dataset>.<table>
```
For our example:
```bash
./bqetl query validate org_mozilla_mozregression_derived.mozregression_aggregates_v1
```
If there are no problems, you should see no output.

## Creating the table schema

Use bqetl to set up the schema that will be used to create the table.

Review the schema.YAML generated as an output of the following command, and make sure all data types are set correctly and according to the data expected from the query.

```bash
./bqetl query schema update <dataset>.<table>`
```

For our example:
```bash
./bqetl query schema update org_mozilla_mozregression_derived.mozregression_aggregates_v1
```

## Creating a DAG

BigQuery-ETL has some facilities in it to automatically add your query to [telemetry-airflow](https://github.com/mozilla/telemetry-airflow) (our instance of Airflow).

Before scheduling your query, you'll need to find an [Airflow DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) to run it off of. In some cases, one may already exist that makes sense to use for your dataset -- look in `dags.yaml` at the root or run `./bqetl dag info`. In this particular case, there's no DAG that really makes sense -- so we'll create a new one:

```bash
./bqetl dag create <dag_name> --schedule-interval "0 4 * * *" --owner <email_for_notifications> --description "Add a clear description of the DAG here" --start-date <YYYY-MM-DD> --tag impact/<tier>
```

For our example, the starting date is `2020-06-01` and we use a schedule interval of `0 4 \* \* \*` (4am UTC daily) instead of "daily" (12am UTC daily) to make sure this isn't competing for slots with desktop and mobile product ETL.

The `--tag impact/tier3` parameter specifies that this DAG is considered "tier 3". For a list of valid tags and their descriptions see [Airflow Tags](../reference/airflow_tags.md).

```bash
./bqetl dag create bqetl_internal_tooling --schedule-interval "0 4 * * *" --owner wlachance@mozilla.com --description "This DAG schedules queries for populating queries related to Mozilla's internal developer tooling (e.g. mozregression)." --start-date 2020-06-01 --tag impact/tier_3
```

## Scheduling your query

Queries are automatically scheduled during creation in the DAG set using the option `--dag`, or in the default DAG `bqetl_default` when this option is not used.

If the query was created with `--no-schedule`, it is possible to manually schedule the query via the `bqetl` tool:

```bash
./bqetl query schedule <dataset>.<table> --dag <dag_name> --task-name <task_name>
```

Here is the command for our example. Notice the name of the table as created with the suffix _v1.
```bash
./bqetl query schedule org_mozilla_mozregression_derived.mozregression_aggregates_v1 --dag bqetl_internal_tooling --task-name mozregression_aggregates__v1
```

Note that we are scheduling the generation of the underlying _table_ which is `org_mozilla_mozregression_derived.mozregression_aggregates_v1` rather than the view.

## Get Data Review

_This is for public datasets only! You can skip this step if you're only creating a dataset for Mozilla-internal use._

Before a dataset can be made public, it needs to go through data review according to our [data publishing process](https://wiki.mozilla.org/Data_Publishing#Dataset_Publishing_Process_2). This means filing a bug, answering a few questions, and then finding a [data steward](https://wiki.mozilla.org/Firefox/Data_Collection) to review your proposal.

The dataset we're using in this example is very simple and straightforward and does not have any particularly sensitive data, so the data review is very simple. You can see the full details in [bug 1691105](https://bugzilla.mozilla.org/show_bug.cgi?id=1691105).

## Create a Pull Request

Now is a good time to create a pull request with your changes to GitHub. This is the usual git workflow:

```bash
git checkout -b <new_branch_name>
git add dags.yaml dags/<dag_name>.py sql/moz-fx-data-shared-prod/telemetry/<view> sql/moz-fx-data-shared-prod/<dataset>/<table>
git commit
git push origin <new_branch_name>
```

And next is the workflow for our specific example:

```bash
git checkout -b mozregression-aggregates
git add dags.yaml dags/bqetl_internal_tooling.py sql/moz-fx-data-shared-prod/org_mozilla_mozregression/mozregression_aggregates sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1
git commit
git push origin mozregression-aggregates
```

Then create your pull request, either from the GitHub web interface or the command line, per your preference.

**Note** At this point, the CI is expected to fail because the schema does not exist yet in BigQuery. This will be handled in the next step.

This example assumes that `origin` points to your fork. Adjust the last push invocation appropriately if you have a different [remote](https://git-scm.com/docs/git-remote) set.

Speaking of forks, note that if you're making this pull request from a fork, many jobs will currently fail due to lack of credentials. In fact, even if you're pushing to the origin, you'll get failures because the table is not yet created. That brings us to the next step, but before going further it's generally best to get someone to review your work: at this point we have more than enough for people to provide good feedback on.

## Creating an initial table

Once the PR has been approved, deploy the schema to bqetl using this command:

```bash
./bqetl query schema deploy <schema>.<table>
```

For our example:
```bash
./bqetl query schema deploy org_mozilla_mozregression_derived.mozregression_aggregates_v1
```

## Backfilling the dataset

It is recommended to use the [bqetl backfill command](https://mozilla.github.io/bigquery-etl/bqetl/#backfill) in order to load the data in your new table, and set specific dates for large sets of data, as well as following the [recommended practices](https://mozilla.github.io/bigquery-etl/reference/recommended_practices/#backfills).

```bash
bqetl query backfill <dataset>.<table> --project_id=moz-fx-data-shared-prod -s <YYYY-MM-DD> -e <YYYY-MM-DD> -n 0
```

For our example:
```bash
./bqetl query backfill org_mozilla_mozregression_derived.mozregression_aggregates_v1 --project_id=moz-fx-data-shared-prod -s 2020-04-01 -e 2021-02-01
```

**Note**. Alternatively, you can trigger the Airflow DAG to backfill the data. In this case, it is recommended to talk to someone in in Data Engineering or Data SRE to trigger the DAG.

## Completing the Pull Request

At this point, the table exists in Bigquery so you are able to:
- [Find and re-run the CI](https://app.circleci.com/pipelines/github/mozilla/bigquery-etl?) of your PR and make sure that all tests pass
- Merge your PR.
