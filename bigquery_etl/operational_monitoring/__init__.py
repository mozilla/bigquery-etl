"""Generate and run the Operational Monitoring Queries."""
import glob
import json
import logging
import os
import re
import time
from datetime import date, datetime
from multiprocessing.pool import ThreadPool
from pathlib import Path

import click
from google.cloud import bigquery, storage

from bigquery_etl.util.common import render, write_sql

PATH = Path(os.path.dirname(__file__))

QUERY_FILENAME = "{}_query{}.sql"
INIT_FILENAME = "{}_init.sql"
VIEW_FILENAME = "{}_view.sql"
BUCKET_NAME = "operational_monitoring"
PROJECTS_FOLDER = "projects/"
OUTPUT_DIR = "sql/moz-fx-data-shared-prod/"
PROD_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_DATASET = "operational_monitoring_derived"

DATA_TYPES = {"histogram", "scalar"}

# See https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
# for reference of where these numbers come from.
USERS_PER_BUILD_THRESHOLDS = {"nightly": 375, "beta": 9000, "release": 625000}

# This is a mapping of project slug to metadata.
om_projects = {}


def _download_json_file(project, bucket, filename):
    blob = bucket.get_blob(filename)
    return json.loads(blob.download_as_bytes()), blob.updated


def _get_name_and_sql(query, reference_content, item_type, filter_type=None):
    return [
        {"name": item, "sql": reference_content[item]["sql"]}
        for item in query[item_type]
        if reference_content[item].get("type") == filter_type
    ]


def _write_sql(
    project, dataset, slug, kwargs, template_filename, output_filename=None, init=False
):
    output_filename = output_filename or template_filename
    write_sql(
        OUTPUT_DIR,
        f"{project}.{dataset}.{slug}",
        output_filename,
        render(
            template_filename,
            template_folder=PATH,
            **kwargs,
            init=init,
        ),
    )


def _bq_normalize_name(name):
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _query_up_to_date(dataset, slug, basename, project_last_modified):
    """Check whether the project file was updated after the query.sql file was written."""
    for data_type in DATA_TYPES:
        formatted_basename = basename.format(data_type)
        query_path = Path(os.path.join(OUTPUT_DIR, dataset, slug, formatted_basename))
        if not os.path.exists(query_path):
            return False

        query_last_modified = datetime.utcfromtimestamp(query_path.stat().st_mtime)
        if query_last_modified < project_last_modified.replace(tzinfo=None):
            return False

    return True


def _write_sql_for_data_type(
    query_id, query, project, dataset, slug, render_kwargs, probes, data_type
):
    probes = _get_name_and_sql(query, probes, "probes", data_type)
    if len(probes) == 0:
        # There are no probes for this data source + data type combo
        return

    normalized_slug = _bq_normalize_name(slug)
    render_kwargs.update(
        {
            "source": query["source"],
            "probes": probes,
            "slug": slug,
        }
    )

    _write_sql(
        project,
        dataset,
        normalized_slug,
        render_kwargs,
        QUERY_FILENAME.format(data_type, ""),
        QUERY_FILENAME.format(data_type, query_id),
    )

    if query_id > 0:
        # We only need to write the view/init files for the first query
        # (query_id == 0). The same view/table will be reused for subsequent
        # queries coming from a different data source.
        return

    # Init and view files need the normalized slug
    render_kwargs.update({"slug": normalized_slug})
    _write_sql(
        project,
        dataset,
        normalized_slug,
        render_kwargs,
        INIT_FILENAME.format(data_type),
        init=True,
    )
    _write_sql(
        project,
        dataset,
        normalized_slug,
        render_kwargs,
        VIEW_FILENAME.format(data_type),
    )


def _generate_sql(project, dataset):
    render_kwargs = {
        "header": "-- Generated via bigquery_etl.operational_monitoring\n",
        "gcp_project": project,
        "dataset": dataset,
    }

    client = storage.Client(project)
    bucket = client.get_bucket(BUCKET_NAME)

    probes, _ = _download_json_file(project, bucket, "probes.json")
    dimensions, _ = _download_json_file(project, bucket, "dimensions.json")

    # Iterating over all defined operational monitoring projects
    for blob in bucket.list_blobs(prefix=PROJECTS_FOLDER):
        # The folder itself is not a project file
        if blob.name == PROJECTS_FOLDER:
            continue

        om_project, project_last_modified = _download_json_file(
            project, bucket, blob.name
        )
        normalized_slug = _bq_normalize_name(om_project["slug"])
        boolean_pref = om_project.get("boolean_pref")
        branches = om_project.get("branches", [])
        channel = om_project.get("channel")

        # Branches should not be defined if a boolean pref is defined
        # since the branches are inferred to be either "enabled" and "disabled"
        assert not (
            branches and boolean_pref
        ), "`branches` should not be defined if `boolean_pref` is defined"
        render_kwargs.update(
            {
                "branches": branches,
                "channel": channel,
                "user_count_threshold": USERS_PER_BUILD_THRESHOLDS[channel],
                "pref": boolean_pref,
                "xaxis": om_project.get("xaxis"),
                "start_date": om_project.get("start_date"),
            }
        )

        # Add xaxis metadata to be used to decide whether the entire table is replaced
        # Or just a partition.
        #
        # Note: there is a subtle design here in which date partitions are replaced
        # if the data is for a build over build analysis but the entire table is
        # replaced if it's a submission date analysis.
        om_projects[normalized_slug] = {"xaxis": om_project["xaxis"]}

        if _query_up_to_date(
            dataset, normalized_slug, INIT_FILENAME, project_last_modified
        ):
            logging.info(
                f"Queries for {normalized_slug} are up to date and will not be regenerated"
            )
            continue

        # Iterating over each dataset to query for a given project.
        for query_id, query in enumerate(om_project["analysis"]):
            render_kwargs.update(
                {"dimensions": _get_name_and_sql(query, dimensions, "dimensions")}
            )
            for data_type in DATA_TYPES:
                _write_sql_for_data_type(
                    query_id,
                    query,
                    project,
                    dataset,
                    om_project["slug"],
                    render_kwargs,
                    probes,
                    data_type,
                )


def _run_sql_for_data_type(
    bq_client, project, dataset, normalized_slug, query_config, data_type
):
    init_sql_path = Path(
        os.path.join(
            OUTPUT_DIR, dataset, normalized_slug, INIT_FILENAME.format(data_type)
        )
    )
    query_sql_path = Path(
        os.path.join(
            OUTPUT_DIR, dataset, normalized_slug, QUERY_FILENAME.format(data_type, "*")
        )
    )
    view_sql_path = Path(
        os.path.join(
            OUTPUT_DIR, dataset, normalized_slug, VIEW_FILENAME.format(data_type)
        )
    )
    init_query_text = init_sql_path.read_text()
    view_text = view_sql_path.read_text()

    # Wait for init to complete before running queries
    init_query_job = bq_client.query(init_query_text)
    view_query_job = bq_client.query(view_text)
    results = init_query_job.result()

    query_files = glob.glob(str(query_sql_path))
    for file_id, query_file in enumerate(query_files):
        query_text = Path(query_file).read_text()
        if file_id > 0:
            # All subsequent files should append their output to the existing table
            query_config.write_disposition = "WRITE_APPEND"

        query_job = bq_client.query(query_text, job_config=query_config)

        # Periodically print so airflow gke operator doesn't think task is dead
        elapsed = 0
        while not query_job.done():
            time.sleep(10)
            elapsed += 10
            if elapsed % 200 == 10:
                print("Waiting on query...")

        print(f"Total elapsed: approximately {elapsed} seconds")
        results = query_job.result()

        print(f"Query job {query_job.job_id} finished")
        print(f"{results.total_rows} rows in {query_config.destination}")

    # Add a view once the derived table is generated.
    view_query_job.result()


def _run_project_sql(bq_client, project, dataset, submission_date, slug):
    normalized_slug = _bq_normalize_name(slug)
    date_partition = str(submission_date).replace("-", "")

    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "submission_date", "DATE", str(submission_date)
            ),
        ],
        use_legacy_sql=False,
        clustering_fields=["build_id"],
        default_dataset=f"{project}.{dataset}",
        time_partitioning=bigquery.TimePartitioning(field="submission_date"),
        use_query_cache=True,
        allow_large_results=True,
    )

    for data_type in DATA_TYPES:
        destination_table = f"{project}.{dataset}.{normalized_slug}_{data_type}"

        if om_projects[normalized_slug]["xaxis"] == "build_id":
            destination_table += f"${date_partition}"

        query_config.destination = destination_table
        query_config.write_disposition = "WRITE_TRUNCATE"

        _run_sql_for_data_type(
            bq_client, project, dataset, normalized_slug, query_config, data_type
        )


def _run_sql(project, dataset, submission_date, parallelism):
    bq_client = bigquery.Client(project=PROD_PROJECT)
    derived_dir = os.path.join(OUTPUT_DIR, dataset)

    with ThreadPool(parallelism) as pool:
        pool.starmap(
            _run_project_sql,
            [
                (bq_client, project, dataset, submission_date, slug)
                for slug in os.listdir(derived_dir)
                if os.path.isdir(os.path.join(derived_dir, slug))
            ],
            chunksize=1,
        )


@click.group(help="Commands for Operational Monitoring ETL.")
def operational_monitoring():
    """Create the CLI group for operational monitoring commands."""
    pass


@operational_monitoring.command("run")
@click.option("--project", default=PROD_PROJECT)
@click.option("--dataset", default=DEFAULT_DATASET)
@click.option(
    "--submission-date",
    required=True,
    type=date.fromisoformat,
)
@click.option(
    "--parallelism",
    default=4,
    help="Maximum number of queries to execute concurrently",
)
def run(project, dataset, submission_date, parallelism):
    """Generate and run the Operational Monitoring Queries."""
    _generate_sql(project, dataset)
    _run_sql(project, dataset, submission_date, parallelism)
