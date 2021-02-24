# A quick guide to creating a derived dataset with BigQuery-ETL

This is designed to be a quick guide around creating a simple derived dataset using bigquery-etl and scheduling it using Airflow so it can be updated on a daily basis. We'll be using a simple test case (a small [Glean](https://mozilla.github.io/glean/) application for which we want to generate an aggregated dataset based on the raw ping data) to illustrate the process. If you are interested in looking at the end result of this, you can view the pull request at [mozilla/bigquery-etl#1760](https://github.com/mozilla/bigquery-etl/pull/1760).

## Background

[mozregression](https://mozilla.github.io/mozregression/) is a developer tool used to help developers and community members bisect builds of Firefox to find a regression range in which a bug was introduced. It forms a key part of our quality assurance process.

While in some ways mozregression might seem dissimilar from, say, Firefox most of the concepts illustrated here will apply straightforwardly to the products we ship to customers which use (or will use) the Glean SDK.

In this case, we want to create a simple table of aggregated metrics related to mozregression's use that we can use to power dashboards (to prioritize feature development internally inside Mozilla), as well as syndication as a [public dataset](https://blog.mozilla.org/data/2020/09/25/data-publishing-mozilla/).

## Initial steps

Set up bigquery-etl on your system per the instructions in the [README.md](https://github.com/mozilla/bigquery-etl/blob/master/README.md).

## Create the Query

First, you'll need to create a query file to work off of. For this step, you'll need to know what you want your derived dataset to be called. In this case, we'll name it `org_mozilla_mozregression_derived.mozregression_aggregates`.

The `org_mozilla_mozregression_derived` part represents a _BigQuery dataset_ (a concept that is essentially a container of tables). By convention, we use the `_derived` postfix to hold derived tables like this one.

Run:

```bash
./bqetl query create org_mozilla_mozregression_derived.mozregression_aggregates
```

This will do a couple things:

- Generate some template files (`metadata.yaml`, `query.sql`) representing a query to build the dataset in `sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1`
- Generate a "view" of the dataset in `sql/moz-fx-data-shared-prod/org_mozilla_mozregression/mozregression_aggregates`.

We generate the view to allow us to have a stable interface, while allowing us to evolve the backend details of a dataset over time. Views are automatically published to the `mozdata` project.

## Fill out the query

The next step is to modify the generated `metadata.yaml` and `query.sql` sections with actual information.

Let's look at the `metadata.yaml` first:

```yaml
friendly_name: mozregression aggregates
description: >
  Aggregated metrics of mozregression usage
labels:
  incremental: true
  public_json: true
  public_bigquery: true
  review_bugs:
    - 1691105
owners:
  - wlachance@mozilla.com
```

Most of the fields are self-explanatory. `incremental` means that the table is updated incrementally, e.g. a new partition gets added/updated to the destination table whenever the query is run. For non-incremental queries the entire destination gets overwritten when the query is executed.

You'll also note the `public_json` and `public_bigquery` fields: these mean that the dataset will be published in both Mozilla's public BigQuery project and a world-accessible JSON endpoint. The `review_bugs` section is required, and refers to the Bugzilla bug where opening this data set up to the public was approved: we'll get to that in a subsequent section.

Now that we've filled out the metadata, we can look into creating a query. In many ways, this is similar to creating a SQL query to run on BigQuery in other contexts (e.g. on sql.telemetry.mozilla.org or the BigQuery console)-- the key difference is that we use a `@submission_date` parameter so that the query can be run on a _day's worth_ of data to update the underlying table incrementally.

After testing our query on sql.telemetry.mozilla.org, we'll write this query out to `query.sql`:

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
./bqetl query validate org_mozilla_mozregression_derived.mozregression_aggregates
```

If there are no problems, you should see no output.

## Creating a DAG

BigQuery-ETL has some facilities in it to automatically add your query to [telemetry-airflow](https://github.com/mozilla/telemetry-airflow) (our instance of Airflow).

Before scheduling your query, you'll need to find an [Airflow DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) to run it off of. In some cases, one may already exist that makes sense to use for your dataset -- look in `dags.yaml` at the root or run `./bqetl dag info`. In this particular case, there's no DAG that really makes sense -- so we'll create a new one:

```bash
./bqetl dag create bqetl_internal_tooling --schedule-interval "0 4 * * *" --owner wlachance@mozilla.com --description "This DAG schedules queries for populating queries related to Mozilla's internal developer tooling (e.g. mozregression)." --start-date 2020-06-01
```

Most of the options above should be self-explanatory. We use a schedule interval of "0 4 \* \* \*" (4am UTC daily) instead of "daily" (12am UTC daily) to make sure this isn't competing for slots with desktop and mobile product ETL.

## Scheduling your query

Once again, you access this functionality via the `bqetl` tool:

```bash
./bqetl query schedule org_mozilla_mozregression_derived.mozregression_aggregates_v1 --dag bqetl_internal_tooling --task-name mozregression_aggregates__v1
```

Note that we are scheduling the generation of the underlying _table_ which is `org_mozilla_mozregression_derived.mozregression_aggregates_v1` rather than the view.

After doing this, you will also want to generate the actual airflow configuration which telemetry-airflow will pick up. Run:

```bash
./bqetl dag generate bqetl_internal_tooling
```

This may take a while, as it currently does a dry run through every query defined in bigquery-etl.

## Get Data Review

_This is for public datasets only! You can skip this step if you're only creating a dataset for Mozilla-internal use._

Before a dataset can be made public, it needs to go through data review according to our [data publishing process](https://wiki.mozilla.org/Data_Publishing#Dataset_Publishing_Process_2). This means filing a bug, answering a few questions, and then finding a [data steward](https://wiki.mozilla.org/Firefox/Data_Collection) to review your proposal.

The dataset we're using in this example is very simple and straightforward and doesn't have any particularly sensitive data, so the data review is very simple. You can see the full details in [bug 1691105](https://bugzilla.mozilla.org/show_bug.cgi?id=1691105).

## Create a Pull Request

Now would be a good time to create a pull request with your changes to GitHub. This is the usual git workflow:

```bash
git checkout -b mozregression-aggregates
git add dags.yaml dags/bqetl_internal_tooling.py sql/moz-fx-data-shared-prod/telemetry/mozregression_aggregates sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1
git commit
git push origin mozregression-aggregates
```

Then create your pull request, either from the GitHub web interface or the command line, per your preference.

Note this example assumes that `origin` points to your fork. Adjust the last push invocation appropriately if you have a different [remote](https://git-scm.com/docs/git-remote) set.

Speaking of forks, note that if you're making this pull request from a fork, many jobs will currently fail due to lack of credentials. In fact, even if you're pushing to the origin, you'll get failures because the table is not yet created. That brings us to the next step, but before going further it's generally best to get someone to review your work: at this point we have more than enough for people to provide good feedback on.

## Creating an initial table

To bootstrap an initial table, the normal best practice is to create another SQL file, similar to the incremental query above, which creates the table. We'll write out another file called `init.sql` in `sql/moz-fx-data-shared-prod/org_mozilla_mozregression_derived/mozregression_aggregates_v1`:

```sql
CREATE OR REPLACE TABLE
  `mozilla-public-data`.org_mozilla_mozregression_derived.mozregression_aggregates
PARTITION BY
  DATE(date)
AS
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
  client_info.app_display_version NOT LIKE '%.dev%'
GROUP BY
  date,
  mozregression_version,
  mozregression_variant,
  app_used,
  os,
  os_version;
```

As the dataset will be published in both Mozilla's public BigQuery project, the table will need to be created in the `mozilla-public-data` project. Once the ETL is running, a view `org_mozilla_mozregression_derived.mozregression_aggregates` to the public table will be automatically created in `moz-fx-data-shared-prod`.

Note the `PARTITION BY DATE(date)` in the statement. This makes it so BigQuery will partition the table by date. This isn't too big a deal for mozregression (where even the size of unaggregated data is very small) but [can be a godsend for datasets where each day is hundreds of gigabytes or terabytes big](https://docs.telemetry.mozilla.org/cookbooks/bigquery/optimization.html).

Go ahead and add this to your pull request. Now that we have an initial table definition, we can create a table using this command (if you're not in data engineering, you might have to get someone to run this for you as it implies modifying what we have in production):

```bash
./bqetl query initialize org_mozilla_mozregression_derived.mozregression_aggregates_v1
```

## Backfilling your dataset

In the above example, we actually created the entire history for the table in the initial query. But in many cases, this is not practical and you'd want to manually backfill the data, day-by-day. There are two options here:

1. Backfill the table by triggering the Airflow DAG
2. Using `bqetl backfill`

The first approach is out of scope for this tutorial: you should talk to someone in Data Engineering or Data SRE if you want to do this. The second approach (which is normally pretty effective, at least if your underlying data isn't **too** big) is relatively straightforward though. Run:

```bash
./bqetl query backfill --start-date 2020-04-01 --end-date 2021-02-01 org_mozilla_mozregression_derived.mozregression_aggregates_v1
```
