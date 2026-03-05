"""Utility functions used in generating usage queries on top of Glean."""

# import glob
# import logging
# import os
import re

import pandas as pd
import requests

# from collections import namedtuple
# from functools import cache
# from pathlib import Path


# from jinja2 import Environment, FileSystemLoader, TemplateNotFound

# from bigquery_etl.config import ConfigLoader
# from bigquery_etl.dryrun import DryRun
# from bigquery_etl.schema import Schema
# from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas
# from bigquery_etl.util.common import get_table_dir, render, write_sql

PROBEINFO_URL = "https://probeinfo.telemetry.mozilla.org"
APP_LISTINGS_URL = f"{PROBEINFO_URL}/v2/glean/app-listings"
# PATH = Path(os.path.dirname(__file__))

# # added as the result of baseline checks being added to the template,
# # these apps do not receive a baseline ping causing a very basic row_count
# # check to fail. For this reason, the checks relying on this ping
# # need to be omitted. For more info see: bug-1868848
# BQETL_CHECKS_SKIP_APPS = ConfigLoader.get(
#     "generate", "glean_usage", "bqetl_checks", "skip_apps", fallback=[]
# )
# BIGCONFIG_SKIP_APPS = ConfigLoader.get(
#     "generate", "glean_usage", "bigconfig", "skip_apps", fallback=[]
# )

# DEPRECATED_APP_LIST = ConfigLoader.get(
#     "generate", "glean_usage", "deprecated_apps", fallback=[]
# )

# APPS_WITH_PROFILE_GROUP_ID = ("firefox_desktop",)


def get_glean_app_metrics(v1_name: str) -> dict[str, dict]:
    """Return a dictionary of the Glean app's metrics."""
    resp = requests.get(f"{PROBEINFO_URL}/glean/{v1_name}/metrics")
    resp.raise_for_status()
    return resp.json()


def create_df(ping_name: str, v1_name: str) -> pd.DataFrame:
    """Create DataFrame

    Args:
        ping_name (str): ping name
        v1_name (str): v1 name

    Returns:
        pd.DataFrame: DataFrame of JSON ping data
    """

    ping_name = "microsurvey"

    app_metrics = get_glean_app_metrics(v1_name)

    sensitive_list = []
    name_list = []
    typed_list = []

    for metric, metric_info in app_metrics.items():

        ping = metric.split(".")[0]
        df = pd.DataFrame()
        if ping == ping_name:
            metric_history = metric_info["history"]
            metric_name = metric_info["name"].split(".")[1]
            name_list.append(metric_name)

            for metric in metric_history:
                sensitive_metric = metric["data_sensitivity"]
                sensitive_list.append(sensitive_metric)

                typed = metric["type"]
                typed_list.append(typed)

    df = pd.DataFrame(
        list(zip(name_list, sensitive_list, typed_list)),
        columns=["metric_name", "sensitivity", "data_type"],
    )

    return df


def enhance_df(
    df: pd.DataFrame, ping_name: str, sensitivity_categories: list
) -> pd.DataFrame:
    """Enhance the DataFrame with other needed and/or useful info

    Args:
        df (pd.DataFrame): DataFrame of JSON ping data
        ping_name (str): ping name
        sensitivity_categories (list): list of sensitive categories

    Returns:
        pd.DataFrame: Enhanced DataFrame
    """

    df["ping"] = ping_name

    df["full_metric_name"] = df.apply(
        lambda row: f"metrics.{row['data_type']}.{ping_name}_{row['metric_name']}",
        axis=1,
    )

    sensitive_match = df["sensitivity"].apply(
        lambda x: bool(set(x) & set(sensitivity_categories))
    )
    df["remove"] = sensitive_match  # whether a specific metric needs removing

    df["metric_type_size"] = df.groupby("data_type").transform("size")
    df["group_size"] = df.groupby(["data_type", "remove"]).transform("size")
    df = df.sort_values(by="data_type")
    df["keep_all"] = ~df.groupby("data_type")["remove"].transform(
        "any"
    )  # whether to keep all items of this metric

    df["drop_all"] = df.groupby("data_type")["remove"].transform(
        "all"
    )  # whether to drop all items of this metric

    return df


def get_all_and_partial_fields_bool(df: pd.DataFrame) -> pd.DataFrame:
    """Derived a "keep bools" DataFrame containing the fields to either drop entirely or partially

    Args:
        df (pd.DataFrame): Enhanced DataFrame

    Returns:
        pd.DataFrame: A "keep bools" DataFrame
    """

    df = df[~df["keep_all"]]

    keep_bools_df = df.groupby("drop_all")["data_type"].unique()

    keep_bools_df = keep_bools_df.to_frame().reset_index()

    return keep_bools_df


def get_full_drop(df: pd.DataFrame) -> str:
    """Create EXCEPT string

    Args:
        df (pd.DataFrame): dataframe

    Returns:
        str: EXCEPT string
    """

    full_drop_df = df[df["drop_all"]]
    full_drop_flat_list = full_drop_df["data_type"].explode().tolist()
    full_drop_string = ", ".join(full_drop_flat_list)

    if full_drop_string:
        exceptions_string = f"EXCEPT({full_drop_string})"
    else:
        exceptions_string = ""

    return exceptions_string


def get_partial_drop(keep_bools_df: pd.DataFrame, original_df: pd.DataFrame) -> str:
    """Create REPLACE structure

    Args:
        df (pd.DataFrame): dataframe

    Returns:
        str: REPLACE structure
    """

    partial_drop_df = keep_bools_df[~keep_bools_df["drop_all"]]

    partial_drop_flat_list = partial_drop_df["data_type"].explode().tolist()

    partial_drop_string_list = []
    for data_type in partial_drop_flat_list:
        filtered_data = original_df[
            (original_df["data_type"] == data_type) & (original_df["remove"] == False)
        ]["full_metric_name"]

        flat_list = filtered_data.tolist()
        partial_drop_string = ",\n\t\t".join(flat_list)

        replace_string = f"""STRUCT(
        {partial_drop_string}
        ) AS {data_type}"""

        partial_drop_string_list.append(replace_string)

    partial_drop_string = ", ".join(partial_drop_string_list)

    return partial_drop_string


def create_remove_values_sql(
    full_drop_string: str, partial_drop_string: str, field_name: str = "metrics"
) -> str:

    remove_value_sql = f"""remove_value AS (
    SELECT
        * REPLACE (
        (
            SELECT AS STRUCT
            {field_name}.*  {full_drop_string}
            REPLACE (
            {partial_drop_string}
            )
        ) AS {field_name}
    )"""

    return remove_value_sql


def create_full_sql(
    bq_dataset_family: str, ping_name: str, remove_values_sql: str
) -> str:
    full_sql = f"""
    CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.{bq_dataset_family}.{ping_name}_redacted`
    AS
    {remove_values_sql}
    FROM
    `moz-fx-data-shared-prod.{bq_dataset_family}.{ping_name}`
    """

    return full_sql


def get_sensitive_metrics_for_ping(
    v1_name: str, ping_name: str, sensitivity_categories: list
):

    df = create_df(ping_name, v1_name)
    df = enhance_df(df, ping_name, sensitivity_categories)
    keep_bools_df = get_all_and_partial_fields_bool(df)

    return df, keep_bools_df


def build_select_clause(
    keep_bools_df, df, bq_dataset_family, ping_name, field_name: str = "metrics"
):
    full_drop_string = get_full_drop(keep_bools_df)

    partial_drop_string = get_partial_drop(keep_bools_df, df)

    remove_values_sql = create_remove_values_sql(
        full_drop_string, partial_drop_string, field_name
    )

    full_sql = create_full_sql(bq_dataset_family, ping_name, remove_values_sql)

    return full_sql


def resolve_v1_name(bq_dataset_family: str) -> str:
    """Given a bq_dataset_family, return the v1_name.`

    Args:
        bq_dataset_family (str): The BQ dataset

    Returns:
        str: v1 name
    """

    app_info = get_app_info()
    v1_name = app_info[bq_dataset_family][0]["v1_name"]

    return v1_name


def get_app_info() -> dict[str, list[dict]]:
    """Return applications from the probeinfo app listings API grouped by app name."""
    resp = requests.get(APP_LISTINGS_URL)
    resp.raise_for_status()
    apps_json = resp.json()
    app_info = {}

    for app in apps_json:
        if app["app_id"].startswith("rally"):
            pass
        elif app["app_name"] not in app_info:
            app_info[app["app_name"]] = [app]
        else:
            app_info[app["app_name"]].append(app)

    return app_info


def write_redacted_view():

    pass


# def write_dataset_metadata(output_dir, full_table_id, derived_dataset_metadata=False):
#     """
#     Add dataset_metadata.yaml to public facing datasets.

#     Does not overwrite existing dataset_metadata.yaml files.
#     """
#     d = Path(os.path.join(output_dir, *list(full_table_id.split(".")[-2:])))
#     d.parent.mkdir(parents=True, exist_ok=True)
#     target = d.parent / "dataset_metadata.yaml"

#     public_facing = all(
#         [postfix not in d.parent.name for postfix in ("_derived", "_stable")]
#     )
#     if (derived_dataset_metadata or public_facing) and not target.exists():
#         env = Environment(loader=FileSystemLoader(PATH / "templates"))
#         if derived_dataset_metadata:
#             dataset_metadata = env.get_template("derived_dataset_metadata.yaml")
#         else:
#             dataset_metadata = env.get_template("dataset_metadata.yaml")
#         rendered = dataset_metadata.render(
#             {
#                 "friendly_name": " ".join(
#                     [p.capitalize() for p in d.parent.name.split("_")]
#                 ),
#                 "dataset": d.parent.name,
#             }
#         )

#         logging.info(f"Writing {target}")
#         target.write_text(rendered)


# def list_tables(project_id, only_tables, table_filter, table_name="baseline_v1"):
#     """Return names of all matching baseline tables in shared-prod."""
#     prod_baseline_tables = [
#         s.stable_table
#         for s in get_stable_table_schemas()
#         if s.schema_id == "moz://mozilla.org/schemas/glean/ping/1"
#         and s.bq_table == table_name
#     ]
#     prod_datasets_with_baseline = [t.split(".")[0] for t in prod_baseline_tables]
#     stable_datasets = prod_datasets_with_baseline
#     if only_tables and not _contains_glob(only_tables):
#         # skip list calls when only_tables exists and contains no globs
#         return [
#             f"{project_id}.{t}"
#             for t in only_tables
#             if table_filter(t) and t in prod_baseline_tables
#         ]
#     if only_tables and not _contains_glob(
#         _extract_dataset_from_glob(t) for t in only_tables
#     ):
#         stable_datasets = {_extract_dataset_from_glob(t) for t in only_tables}
#         stable_datasets = {
#             d
#             for d in stable_datasets
#             if d.endswith("_stable") and d in prod_datasets_with_baseline
#         }
#     return [
#         f"{project_id}.{d}.{table_name}"
#         for d in stable_datasets
#         if table_filter(f"{d}.{table_name}")
#     ]


# @cache
# def _get_pings_without_metrics():
#     def metrics_in_schema(fields):
#         for field in fields:
#             if field["name"] == "metrics":
#                 return True
#         return False

#     return {
#         (schema.bq_dataset_family, schema.bq_table_unversioned)
#         for schema in get_stable_table_schemas()
#         if "glean" in schema.schema_id and not metrics_in_schema(schema.schema)
#     }


# def ping_has_metrics(dataset_family: str, unversioned_table: str) -> bool:
#     """Return true if the given stable table schema has a metrics column at the top-level."""
#     return (dataset_family, unversioned_table) not in _get_pings_without_metrics()


# def table_names_from_baseline(baseline_table, include_project_id=True):
#     """Return a dict with full table IDs for derived tables and views.

#     :param baseline_table: stable table ID in project.dataset.table form
#     """
#     prefix = re.sub(r"_stable\..+", "", baseline_table)
#     if not include_project_id:
#         prefix = ".".join(prefix.split(".")[1:])
#     return dict(
#         baseline_table=f"{prefix}_stable.baseline_v1",
#         migration_table=f"{prefix}_stable.migration_v1",
#         daily_table=f"{prefix}_derived.baseline_clients_daily_v1",
#         last_seen_table=f"{prefix}_derived.baseline_clients_last_seen_v1",
#         first_seen_table=f"{prefix}_derived.baseline_clients_first_seen_v1",
#         daily_view=f"{prefix}.baseline_clients_daily",
#         last_seen_view=f"{prefix}.baseline_clients_last_seen",
#         first_seen_view=f"{prefix}.baseline_clients_first_seen",
#         event_monitoring=f"{prefix}_derived.event_monitoring_live_v1",
#         events_view=f"{prefix}.events",
#         events_stream_table=f"{prefix}_derived.events_stream_v1",
#         events_stream_view=f"{prefix}.events_stream",
#         events_first_seen_table=f"{prefix}_derived.events_first_seen_v1",
#         events_first_seen_view=f"{prefix}.events_first_seen",
#     )


# def _contains_glob(patterns):
#     return any({"*", "?", "["}.intersection(pattern) for pattern in patterns)


# def _extract_dataset_from_glob(pattern):
#     # Assumes globs are in <dataset>.<table> form without a project specified.
#     return pattern.split(".", 1)[0]


# @cache
# def get_glean_repositories() -> list[dict]:
#     """Return a list of the Glean repositories."""
#     resp = requests.get(f"{PROBEINFO_URL}/glean/repositories")
#     resp.raise_for_status()
#     return resp.json()


# def get_glean_app_pings(v1_name: str) -> dict[str, dict]:
#     """Return a dictionary of the Glean app's pings."""
#     resp = requests.get(f"{PROBEINFO_URL}/glean/{v1_name}/pings")
#     resp.raise_for_status()
#     return resp.json()


# class GleanTable:
#     """Represents a generated Glean table."""

#     def __init__(self):
#         """Init Glean table."""
#         self.target_table_id = ""
#         self.prefix = ""
#         self.common_render_kwargs = {}
#         self.per_app_id_enabled = True
#         self.per_app_enabled = True
#         self.per_app_requires_all_base_tables = False
#         self.across_apps_enabled = True
#         self.cross_channel_template = "cross_channel.view.sql"
#         self.base_table_name = "baseline_v1"
#         self.possible_query_parameters = {"submission_date": "DATE"}

#     def skip_existing(self, output_dir="sql/", project_id="moz-fx-data-shared-prod"):
#         """Existing files configured not to be overridden during generation."""
#         return [
#             file.replace(
#                 f'{ConfigLoader.get("default", "sql_dir", fallback="sql/")}{project_id}',
#                 str(output_dir),
#             )
#             for skip_existing in ConfigLoader.get(
#                 "generate", "glean_usage", "skip_existing", fallback=[]
#             )
#             for file in glob.glob(skip_existing, recursive=True)
#         ]

#     def generate_per_app_id(
#         self,
#         project_id,
#         baseline_table,
#         app_name,
#         app_id_info,
#         output_dir=None,
#         use_cloud_function=True,
#         parallelism=8,
#         id_token=None,
#         custom_render_kwargs=None,
#         use_python_query=False,
#     ):
#         """Generate the baseline table query per app_id."""
#         if not self.per_app_id_enabled:
#             return

#         tables = table_names_from_baseline(baseline_table, include_project_id=False)

#         query_filename = f"{self.target_table_id}.query.sql"
#         python_query_filename = f"{self.target_table_id}.query.py"
#         checks_filename = f"{self.target_table_id}.checks.sql"
#         bigconfig_filename = f"{self.target_table_id}.bigconfig.yml"
#         view_filename = f"{self.target_table_id[:-3]}.view.sql"
#         view_metadata_filename = f"{self.target_table_id[:-3]}.metadata.yaml"
#         table_metadata_filename = f"{self.target_table_id}.metadata.yaml"
#         schema_filename = f"{self.target_table_id}.schema.yaml"

#         table = tables[f"{self.prefix}_table"]
#         view = tables[f"{self.prefix}_view"]
#         derived_dataset = tables["daily_table"].split(".")[-2]

#         # Some apps did briefly send a baseline ping,
#         # but do not do so actively anymore. This is why they get excluded.
#         enable_monitoring = app_name not in list(set(BIGCONFIG_SKIP_APPS))

#         # Some apps' tables have been deprecated
#         deprecated_app = app_name in list(set(DEPRECATED_APP_LIST))

#         render_kwargs = dict(
#             header="-- Generated via bigquery_etl.glean_usage\n",
#             header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
#             project_id=project_id,
#             derived_dataset=derived_dataset,
#             target_table=self.target_table_id,
#             app_name=app_name,
#             has_profile_group_id=app_name in APPS_WITH_PROFILE_GROUP_ID,
#             enable_monitoring=enable_monitoring,
#             deprecated_app=deprecated_app,
#         )

#         render_kwargs.update(self.common_render_kwargs)
#         render_kwargs.update(tables)
#         if custom_render_kwargs:
#             render_kwargs.update(custom_render_kwargs)

#         # query.sql is optional for python queries
#         query_sql = None
#         query_python = None
#         if (PATH / "templates" / query_filename).exists() or not use_python_query:
#             query_sql = render(
#                 query_filename, template_folder=PATH / "templates", **render_kwargs
#             )
#         if use_python_query:
#             query_python = render(
#                 python_query_filename,
#                 template_folder=PATH / "templates",
#                 format=False,
#                 **render_kwargs,
#             )
#         view_sql = render(
#             view_filename, template_folder=PATH / "templates", **render_kwargs
#         )

#         view_metadata = render(
#             view_metadata_filename,
#             template_folder=PATH / "templates",
#             format=False,
#             **render_kwargs,
#         )
#         table_metadata = render(
#             table_metadata_filename,
#             template_folder=PATH / "templates",
#             format=False,
#             **render_kwargs,
#         )

#         # Checks are optional, for now!
#         try:
#             checks_sql = render(
#                 checks_filename, template_folder=PATH / "templates", **render_kwargs
#             )
#         except TemplateNotFound:
#             checks_sql = None

#         # Schema files are optional, except for Python queries without a SQL query to dry run
#         try:
#             schema = render(
#                 schema_filename,
#                 format=False,
#                 template_folder=PATH / "templates",
#                 **render_kwargs,
#             )
#         except TemplateNotFound:
#             schema = None
#             if query_python:
#                 if query_sql:
#                     schema = Schema(
#                         DryRun(
#                             os.path.join(project_id, *table.split("."), "query.sql"),
#                             content=query_sql,
#                             query_parameters=self.possible_query_parameters,
#                             use_cloud_function=use_cloud_function,
#                             id_token=id_token,
#                         ).get_schema()
#                     ).to_yaml()
#                 else:
#                     raise

#         if enable_monitoring:
#             try:
#                 bigconfig_contents = render(
#                     bigconfig_filename,
#                     format=False,
#                     template_folder=PATH / "templates",
#                     **render_kwargs,
#                 )
#             except TemplateNotFound:
#                 bigconfig_contents = None
#         else:
#             bigconfig_contents = None

#         # generated files to update
#         Artifact = namedtuple("Artifact", "table_id basename sql")
#         artifacts = [
#             Artifact(view, "metadata.yaml", view_metadata),
#             Artifact(table, "metadata.yaml", table_metadata),
#         ]
#         if query_sql:
#             artifacts.append(
#                 Artifact(
#                     table,
#                     "query_supplemental.sql" if query_python else "query.sql",
#                     query_sql,
#                 )
#             )
#         if query_python:
#             artifacts.append(Artifact(table, "query.py", query_python))

#         artifacts.append(Artifact(view, "view.sql", view_sql))

#         skip_existing_artifact = self.skip_existing(output_dir, project_id)

#         if output_dir:
#             if checks_sql:
#                 if "baseline" in table and app_name in BQETL_CHECKS_SKIP_APPS:
#                     logging.info(
#                         "Skipped copying ETL check for %s as app: %s is marked as not having baseline ping"
#                         % (table, app_name)
#                     )
#                 else:
#                     if checks_sql:
#                         artifacts.append(Artifact(table, "checks.sql", checks_sql))

#             if bigconfig_contents:
#                 artifacts.append(Artifact(table, "bigconfig.yml", bigconfig_contents))

#             if schema:
#                 artifacts.append(Artifact(table, "schema.yaml", schema))

#             for artifact in artifacts:
#                 destination = (
#                     get_table_dir(output_dir, artifact.table_id) / artifact.basename
#                 )
#                 skip_existing = str(destination) in skip_existing_artifact

#                 write_sql(
#                     output_dir,
#                     artifact.table_id,
#                     artifact.basename,
#                     artifact.sql,
#                     skip_existing=skip_existing,
#                 )

#             write_dataset_metadata(output_dir, view)

#     def generate_per_app(
#         self,
#         project_id,
#         app_name,
#         app_ids_info,
#         output_dir=None,
#         use_cloud_function=True,
#         parallelism=8,
#         id_token=None,
#         all_base_tables_exist=None,
#         custom_render_kwargs=None,
#     ):
#         """Generate the baseline table query per app_name."""
#         if not self.per_app_enabled:
#             return

#         target_view_name = "_".join(self.target_table_id.split("_")[:-1])
#         target_dataset = app_name

#         if self.per_app_requires_all_base_tables and not all_base_tables_exist:
#             logging.info(
#                 f"Skipping per-app generation for {target_dataset}.{target_view_name} as not all baseline tables exist"
#             )
#             return

#         datasets = [
#             (a["bq_dataset_family"], a.get("app_channel", "release"))
#             for a in app_ids_info
#         ]

#         if len(datasets) == 1 and target_dataset == datasets[0][0]:
#             # This app only has a single channel, and the app_name
#             # exactly matches the generated bq_dataset_family, so
#             # the existing per-app_id dataset also serves as the
#             # per-app dataset, thus we don't have to provision
#             # union views.
#             if self.per_app_id_enabled:
#                 return

#         enable_monitoring = app_name not in list(set(BIGCONFIG_SKIP_APPS))

#         # Some apps' tables have been deprecated
#         deprecated_app = app_name in list(set(DEPRECATED_APP_LIST))

#         render_kwargs = dict(
#             header="-- Generated via bigquery_etl.glean_usage\n",
#             header_yaml="---\n# Generated via bigquery_etl.glean_usage\n",
#             project_id=project_id,
#             target_view=f"{target_dataset}.{target_view_name}",
#             datasets=datasets,
#             table=target_view_name,
#             target_table=f"{target_dataset}_derived.{self.target_table_id}",
#             app_name=app_name,
#             enable_monitoring=enable_monitoring,
#             deprecated_app=deprecated_app,
#         )
#         render_kwargs.update(self.common_render_kwargs)
#         if custom_render_kwargs:
#             render_kwargs.update(custom_render_kwargs)

#         skip_existing_artifacts = self.skip_existing(output_dir, project_id)

#         Artifact = namedtuple("Artifact", "table_id basename sql")

#         if self.cross_channel_template:
#             sql = render(
#                 self.cross_channel_template,
#                 template_folder=PATH / "templates",
#                 **render_kwargs,
#             )
#             view = f"{project_id}.{target_dataset}.{target_view_name}"

#             if output_dir:
#                 write_dataset_metadata(output_dir, view)

#             if output_dir:
#                 skip_existing = (
#                     str(get_table_dir(output_dir, view) / "view.sql")
#                     in skip_existing_artifacts
#                 )
#                 write_sql(
#                     output_dir, view, "view.sql", sql, skip_existing=skip_existing
#                 )
#         else:
#             query_filename = f"{target_view_name}.query.sql"
#             query_sql = render(
#                 query_filename, template_folder=PATH / "templates", **render_kwargs
#             )
#             view_sql = render(
#                 f"{target_view_name}.view.sql",
#                 template_folder=PATH / "templates",
#                 **render_kwargs,
#             )
#             metadata = render(
#                 f"{self.target_table_id[:-3]}.metadata.yaml",
#                 template_folder=PATH / "templates",
#                 format=False,
#                 **render_kwargs,
#             )

#             table = f"{project_id}.{target_dataset}_derived.{self.target_table_id}"
#             view = f"{project_id}.{target_dataset}.{target_view_name}"

#             if output_dir:
#                 artifacts = [
#                     Artifact(table, "query.sql", query_sql),
#                     Artifact(table, "metadata.yaml", metadata),
#                     Artifact(view, "view.sql", view_sql),
#                 ]

#                 if (
#                     self.target_table_id.startswith("metrics_clients_")
#                     and enable_monitoring
#                 ):
#                     bigconfig_contents = render(
#                         f"{self.target_table_id[:-3]}.bigconfig.yml",
#                         format=False,
#                         template_folder=PATH / "templates",
#                         **render_kwargs,
#                     )

#                     artifacts.append(
#                         Artifact(table, "bigconfig.yml", bigconfig_contents)
#                     )

#                 for artifact in artifacts:
#                     destination = (
#                         get_table_dir(output_dir, artifact.table_id) / artifact.basename
#                     )
#                     skip_existing = destination in skip_existing_artifacts

#                     write_sql(
#                         output_dir,
#                         artifact.table_id,
#                         artifact.basename,
#                         artifact.sql,
#                         skip_existing=skip_existing,
#                     )

#                 write_dataset_metadata(output_dir, view)
#                 write_dataset_metadata(output_dir, table, derived_dataset_metadata=True)

#     def generate_across_apps(
#         self, project_id, apps, output_dir=None, use_cloud_function=True, parallelism=8
#     ):
#         """Generate a query across all apps."""
#         # logic for implementing cross-app queries needs to be implemented in the
#         # individual classes
#         return
