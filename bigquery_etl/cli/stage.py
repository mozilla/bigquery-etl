"""bigquery-etl CLI stage commands."""

import re
import shutil
import tempfile
from datetime import datetime
from glob import glob
from pathlib import Path

import rich_click as click
from google.cloud import bigquery
from google.cloud.bigquery.enums import EntityTypes
from google.cloud.exceptions import NotFound

from .. import ConfigLoader
from ..cli.routine import publish as publish_routine
from ..cli.utils import paths_matching_name_pattern, sql_dir_option
from ..dependency import extract_table_references
from ..dryrun import DryRun, get_id_token
from ..routine.parse_routine import (
    ROUTINE_FILES,
    UDF_FILE,
    RawRoutine,
    accumulate_dependencies,
    read_routine_dir,
)
from ..schema import SCHEMA_FILE, Schema
from ..util.common import block_coding_agents, render
from ..view import View

VIEW_FILE = "view.sql"
QUERY_FILE = "query.sql"
QUERY_SCRIPT = "query.py"
MATERIALIZED_VIEW = "materialized_view.sql"
ROOT = Path(__file__).parent.parent.parent
TEST_DIR = ROOT / "tests" / "sql"


@click.group(help="Commands for managing stage deploys")
def stage():
    """Create the CLI group for the stage command."""
    pass


@stage.command(help="""
    Deploy artifacts to the configured stage project. The order of deployment is:
    UDFs, views, tables.

    Coding agents aren't allowed to run this command.

    Examples:
    ./bqetl stage deploy sql/moz-fx-data-shared-prod/telemetry_derived/

    # Deploy with custom test directory
    ./bqetl stage deploy --test-dir /path/to/tests sql/moz-fx-data-shared-prod/telemetry_derived/
    """)
@block_coding_agents
@click.argument(
    "paths",
    nargs=-1,
)
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project to deploy artifacts to",
    default="moz-fx-data-integration-tests",
)
@sql_dir_option
@click.option(
    "--dataset-suffix",
    "--dataset_suffix",
    help="Suffix appended to the deployed dataset",
)
@click.option(
    "--update-references",
    "--update_references",
    default=True,
    is_flag=True,
    help="Update references to deployed artifacts with project and dataset artifacts have been deployed to.",
)
@click.option(
    "--copy-sql-to-tmp-dir",
    "--copy_sql_to_tmp_dir",
    help="Copy existing SQL from the sql_dir to a temporary directory and apply updates there.",
    default=False,
    is_flag=True,
)
@click.option(
    "--remove-updated-artifacts",
    "--remove_updated_artifacts",
    default=False,
    is_flag=True,
    help="Remove artifacts that have been updated and deployed to stage from prod folder. This ensures that"
    + " tests don't run on outdated or undeployed artifacts (required for CI)",
)
@click.option(
    "--test-dir",
    "--test_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=TEST_DIR,
    help="Directory containing test files (default: tests/sql relative to repository root)",
)
@click.pass_context
def deploy(
    ctx,
    paths,
    project_id,
    sql_dir,
    dataset_suffix,
    update_references,
    copy_sql_to_tmp_dir,
    remove_updated_artifacts,
    test_dir,
):
    """Deploy provided artifacts to destination project."""
    if copy_sql_to_tmp_dir:
        # copy SQL to a temporary directory
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_dir.mkdir(parents=True, exist_ok=True)
        new_sql_dir = tmp_dir / Path(sql_dir).name
        shutil.copytree(sql_dir, new_sql_dir, dirs_exist_ok=True)

        # rename paths to tmp_dir
        paths = [path.replace(sql_dir, f"{new_sql_dir}/", 1) for path in paths]

        sql_dir = new_sql_dir

    artifact_files = set()

    # get SQL files for artifacts that are to be deployed
    for path in paths:
        artifact_files.update(
            [
                p
                for p in paths_matching_name_pattern(
                    path, sql_dir, None, files=["*.sql", "*.py"]
                )
                if p.suffix in [".sql", ".py"]
                and p.name != "checks.sql"
                and len(p.suffixes) == 1
            ]
        )

    # any dependencies need to be determined an deployed as well since the stage
    # environment doesn't have access to the prod environment
    artifact_files.update(_udf_dependencies(artifact_files))
    artifact_files.update(_collect_artifact_dependencies(artifact_files, sql_dir))

    # update references of all deployed artifacts
    # references needs to be set to the stage project and the new dataset identifier
    if update_references:
        _update_references(artifact_files, project_id, dataset_suffix, sql_dir)

    updated_artifact_files = set()
    (Path(sql_dir) / project_id).mkdir(parents=True, exist_ok=True)
    # copy updated files locally to a folder representing the stage env project
    for artifact_file in artifact_files:
        artifact_project = artifact_file.parent.parent.parent.name
        artifact_dataset = artifact_file.parent.parent.name
        artifact_name = artifact_file.parent.name
        artifact_test_path = (
            test_dir / artifact_project / artifact_dataset / artifact_name
        )

        if (
            artifact_dataset == "INFORMATION_SCHEMA"
            or "INFORMATION_SCHEMA" in artifact_name
        ):
            continue

        new_artifact_dataset = (
            f"{artifact_dataset}_{artifact_project.replace('-', '_')}"
        )
        if dataset_suffix:
            new_artifact_dataset = f"{new_artifact_dataset}_{dataset_suffix}"

        if artifact_file.name == MATERIALIZED_VIEW:
            # replace CREATE MATERIALIED VIEW statement
            sql_content = render(
                artifact_file.name,
                template_folder=str(artifact_file.parent),
                format=False,
            )
            sql_content = re.sub(
                "CREATE MATERIALIZED VIEW.*?AS",
                "",
                sql_content,
                flags=re.DOTALL,
            )
            artifact_file.write_text(sql_content)
            # map materialized views to normal queries
            query_path = Path(artifact_file.parent, QUERY_FILE)
            artifact_file.rename(query_path)
            artifact_file = query_path

        new_artifact_path = (
            Path(sql_dir) / project_id / new_artifact_dataset / artifact_name
        )
        new_artifact_path.mkdir(parents=True, exist_ok=True)
        shutil.copytree(artifact_file.parent, new_artifact_path, dirs_exist_ok=True)
        updated_artifact_files.add(new_artifact_path / artifact_file.name)

        # copy tests to the right structure
        if artifact_test_path.exists():
            new_artifact_test_path = (
                test_dir / project_id / new_artifact_dataset / artifact_name
            )
            shutil.copytree(
                artifact_test_path, new_artifact_test_path, dirs_exist_ok=True
            )
            if remove_updated_artifacts:
                shutil.rmtree(artifact_test_path)

    # rename test files
    for test_file_path in map(Path, glob(f"{test_dir}/**/*", recursive=True)):
        test_file_suffix = test_file_path.suffix
        for artifact_file in artifact_files:
            artifact_project = artifact_file.parent.parent.parent.name
            artifact_dataset = artifact_file.parent.parent.name
            artifact_name = artifact_file.parent.name
            if test_file_path.name in (
                f"{artifact_project}.{artifact_dataset}.{artifact_name}{test_file_suffix}",
                f"{artifact_project}.{artifact_dataset}.{artifact_name}.schema{test_file_suffix}",
            ) or (
                test_file_path.name
                in (
                    f"{artifact_dataset}.{artifact_name}{test_file_suffix}",
                    f"{artifact_dataset}.{artifact_name}.schema{test_file_suffix}",
                )
                and artifact_project in test_file_path.parent.parts
            ):
                new_artifact_dataset = (
                    f"{artifact_dataset}_{artifact_project.replace('-', '_')}"
                )
                if dataset_suffix:
                    new_artifact_dataset = f"{new_artifact_dataset}_{dataset_suffix}"

                new_test_file_name = (
                    f"{project_id}.{new_artifact_dataset}.{artifact_name}"
                )
                if test_file_path.name.endswith(f".schema{test_file_suffix}"):
                    new_test_file_name += ".schema"
                new_test_file_name += test_file_suffix

                new_test_file_path = test_file_path.parent / new_test_file_name
                if not new_test_file_path.exists():
                    test_file_path.rename(new_test_file_path)
                break

    # remove artifacts from the "prod" folders
    if remove_updated_artifacts:
        for artifact_file in artifact_files:
            if artifact_file.parent.exists():
                shutil.rmtree(artifact_file.parent)

    # deploy to stage
    _deploy_artifacts(ctx, updated_artifact_files, project_id, dataset_suffix, sql_dir)


def _udf_dependencies(artifact_files):
    """Determine UDF dependencies."""
    udf_dependencies = set()
    raw_routines = read_routine_dir()
    udf_files = [file for file in artifact_files if file.name == UDF_FILE]
    for udf_file in udf_files:
        # all referenced UDFs need to be deployed in the same stage project due to access restrictions
        raw_routine = RawRoutine.from_file(udf_file)
        udfs_to_publish = accumulate_dependencies([], raw_routines, raw_routine.name)

        for dependency in udfs_to_publish:
            if dependency in raw_routines:
                file_path = Path(raw_routines[dependency].filepath)
                udf_dependencies.add(file_path)

    return udf_dependencies


def _collect_artifact_dependencies(artifact_files, sql_dir):
    """Determine view and table dependencies.

    Extracts dependencies from artifacts to ensure all referenced artifacts
    are deployed to stage environment.

    For views: Always extracts table references and UDF references, since views
    need their dependencies to exist when created.

    For query files (tables): Only extracts references if there's no schema.yaml.
    With schema.yaml: We deploy the schema structure (not run the query), so
    we don't need the query's dependencies.
    """
    artifact_dependencies = set()
    dependency_files = [
        file
        for file in artifact_files
        if file.name in [VIEW_FILE, QUERY_FILE, MATERIALIZED_VIEW]
    ]
    id_token = get_id_token()

    for dep_file in dependency_files:
        # all referenced views and tables need to be deployed in the same stage project
        if dep_file not in artifact_files:
            artifact_dependencies.add(dep_file)

        if dep_file.name == VIEW_FILE:
            view = View.from_file(dep_file, id_token=id_token)
            table_references = view.table_references
        elif dep_file.name in [QUERY_FILE, QUERY_SCRIPT, MATERIALIZED_VIEW]:
            # for query files, only extract references if there's no schema.yaml
            # if schema.yaml exists, we deploy the schema structure, not run the query,
            # so we don't need the query's dependencies
            schema_file = dep_file.parent / SCHEMA_FILE
            if not schema_file.exists():
                try:
                    sql_content = render(dep_file.name, template_folder=dep_file.parent)
                    table_references = extract_table_references(sql_content)
                except Exception as e:
                    print(f"Warning: Could not extract references from {dep_file}: {e}")
                    table_references = []
            else:
                table_references = []
        else:
            table_references = []

        if table_references:
            # Get the project from the artifact file path for INFORMATION_SCHEMA references
            artifact_project = dep_file.parent.parent.parent.name

            for dependency in table_references:
                dependency_components = dependency.split(".")
                if dependency_components[1:2] == ["INFORMATION_SCHEMA"]:
                    dependency_components.insert(0, artifact_project)
                if dependency_components[2:3] == ["INFORMATION_SCHEMA"]:
                    # INFORMATION_SCHEMA has more components that will be treated as the table name
                    # no deploys for INFORMATION_SCHEMA will happen later on
                    dependency_components = dependency_components[:2] + [
                        ".".join(dependency_components[2:])
                    ]
                if len(dependency_components) != 3:
                    raise ValueError(
                        f"Invalid table reference {dependency} in {dep_file}. "
                        "Tables should be fully qualified, expected format: project.dataset.table."
                    )
                project, dataset, name = dependency_components

                file_path = Path(sql_dir) / project / dataset / name

                file_exists_for_dependency = False
                for file in [VIEW_FILE, QUERY_FILE, QUERY_SCRIPT, MATERIALIZED_VIEW]:
                    if (file_path / file).is_file():
                        if (file_path / file) not in artifact_files:
                            dependency_files.append(file_path / file)

                        file_exists_for_dependency = True
                        break

                path = Path(sql_dir) / project / dataset / name
                if "*" in name:
                    # deploy stub for wildcard tables
                    path = (
                        Path(sql_dir)
                        / project
                        / dataset
                        / name.replace("*", "wildcard")
                    )
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    # don't create schema for wildcard and metadata tables
                    # Create schemas for syndicated tables, stable, live tables and other
                    # tables not managed by bigquery-etl by doing a dryrun.
                    # The stage project doesn't have access to prod tables (e.g when referenced)
                    # so we need to create the schema here and deploy it.
                    if name != "INFORMATION_SCHEMA":
                        partitioned_by = None

                        if any(
                            dataset.endswith(suffix) for suffix in ("_live", "_stable")
                        ):
                            partitioned_by = "submission_timestamp"

                        schema = Schema.for_table(
                            project=project,
                            dataset=dataset,
                            table=name,
                            id_token=id_token,
                            partitioned_by=partitioned_by,
                        )
                        schema.to_yaml_file(path / SCHEMA_FILE)

                if not file_exists_for_dependency:
                    (path / QUERY_SCRIPT).write_text(
                        "# Table stub generated by stage deploy"
                    )
                    artifact_dependencies.add(path / QUERY_SCRIPT)

        # Extract UDF dependencies only for views (not for query files)
        if dep_file.name == VIEW_FILE and table_references:
            raw_routines = read_routine_dir()
            udf_dependencies = set()

            for udf_dependency in view.udf_references:
                routine = raw_routines[udf_dependency]
                udf_dependencies.add(Path(routine.filepath))

            # determine UDF dependencies recursively
            artifact_dependencies.update(_udf_dependencies(udf_dependencies))
            artifact_dependencies.update(udf_dependencies)

    return artifact_dependencies


def _update_references(artifact_files, project_id, dataset_suffix, sql_dir):
    replace_references = []
    replace_partial_references = []
    for artifact_file in artifact_files:
        name = artifact_file.parent.name
        name_pattern = name.replace("*", r"\*")  # match literal *
        original_dataset = artifact_file.parent.parent.name
        original_project = artifact_file.parent.parent.parent.name

        deployed_dataset = original_dataset

        if original_dataset not in (
            "INFORMATION_SCHEMA",
            "region-eu",
            "region-us",
        ):
            deployed_dataset += f"_{original_project.replace('-', '_')}"
            if dataset_suffix:
                deployed_dataset += f"_{dataset_suffix}"

        deployed_project = project_id

        # Replace references, preserving fully quoted references.
        replace_partial_references += [
            # partially qualified references (like "telemetry.main")
            (
                re.compile(rf"(?<![\._])`{original_dataset}\.{name_pattern}`"),
                f"`{deployed_project}.{deployed_dataset}.{name}`",
                original_project,
            ),
            (
                re.compile(
                    rf"(?<![\._])`?{original_dataset}`?\.`?{name_pattern}(?![a-zA-Z0-9_])`?"
                ),
                f"`{deployed_project}`.`{deployed_dataset}`.`{name}`",
                original_project,
            ),
        ]
        replace_references += [
            # fully qualified references (like "moz-fx-data-shared-prod.telemetry.main")
            (
                re.compile(
                    rf"`{original_project}\.{original_dataset}\.{name_pattern}`"
                ),
                f"`{deployed_project}.{deployed_dataset}.{name}`",
                original_project,
            ),
            (
                re.compile(
                    rf"(?<![a-zA-Z0-9_])`?{original_project}`?\.`?{original_dataset}`?\.`?{name_pattern}(?![a-zA-Z0-9_])`?"
                ),
                f"`{deployed_project}`.`{deployed_dataset}`.`{name}`",
                original_project,
            ),
        ]

    for path in map(Path, glob(f"{sql_dir}/**/*.sql", recursive=True)):
        # apply substitutions
        if path.is_file():
            if "is_init()" in path.read_text():
                init_sql = render(
                    path.name,
                    template_folder=path.parent,
                    format=False,
                    **{"is_init": lambda: True},
                )
                query_sql = render(
                    path.name,
                    template_folder=path.parent,
                    format=False,
                    **{"is_init": lambda: False},
                )
                sql = f"""
                    {{% if is_init() %}}
                    {init_sql}
                    {{% else %}}
                    {query_sql}
                    {{% endif %}}
                """
            else:
                sql = render(path.name, template_folder=path.parent, format=False)

            for ref in replace_references:
                sql = re.sub(ref[0], ref[1], sql)

            for ref in replace_partial_references:
                file_project = path.parent.parent.parent.name
                if file_project == ref[2]:
                    sql = re.sub(ref[0], ref[1], sql)

            path.write_text(sql)


def _deploy_artifacts(ctx, artifact_files, project_id, dataset_suffix, sql_dir):
    """Deploy routines, tables and views."""
    # give read permissions to dry run accounts
    dataset_access_entries = [
        bigquery.AccessEntry(
            role="READER",
            entity_type=EntityTypes.USER_BY_EMAIL,
            entity_id=dry_run_account,
        )
        for dry_run_account in ConfigLoader.get(
            "dry_run", "function_accounts", fallback=[]
        )
    ]

    # deploy routines
    routine_files = [file for file in artifact_files if file.name in ROUTINE_FILES]
    for routine_file in routine_files:
        dataset = routine_file.parent.parent.name
        create_dataset_if_not_exists(
            project_id=project_id,
            dataset=dataset,
            suffix=dataset_suffix,
            access_entries=dataset_access_entries,
        )
    ctx.invoke(publish_routine, name=None, project_id=project_id, dry_run=False)

    query_files = list(
        {
            file
            for file in artifact_files
            if file.name in [QUERY_FILE, QUERY_SCRIPT]
            # don't attempt to deploy wildcard or metadata tables
            and "*" not in file.parent.name and file.parent.name != "INFORMATION_SCHEMA"
        }
    )

    view_files = [
        file
        for file in artifact_files
        if file.name == VIEW_FILE and str(file) not in DryRun.skipped_files()
    ]

    all_artifacts = query_files + view_files
    for artifact_file in all_artifacts:
        dataset = artifact_file.parent.parent.name
        create_dataset_if_not_exists(
            project_id=project_id,
            dataset=dataset,
            suffix=dataset_suffix,
            access_entries=dataset_access_entries,
        )

    if len(all_artifacts) > 0:
        from ..cli.deploy import deploy as deploy_artifacts_unified

        artifact_paths = [str(f.parent) for f in all_artifacts]

        ctx.invoke(
            deploy_artifacts_unified,
            paths=tuple(set(artifact_paths)),
            tables=len(query_files) > 0,
            views=len(view_files) > 0,
            sql_dir=sql_dir,
            project_ids=[project_id],
            parallelism=8,
            dry_run=False,
            respect_dryrun_skip=False,
            use_cloud_function=True,
            # Table options
            table_force=True,
            table_skip_existing=False,
            table_skip_external_data=True,
            table_skip_existing_schemas=True,
            # View options
            view_force=True,
            view_target_project=None,
            view_add_managed_label=False,
            view_skip_authorized=False,
            view_authorized_only=False,
        )


def create_dataset_if_not_exists(project_id, dataset, suffix=None, access_entries=None):
    """Create a temporary dataset if not already exists."""
    client = bigquery.Client(project_id)
    dataset = bigquery.Dataset(f"{project_id}.{dataset}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = 60 * 60 * 1000 * 12  # ms

    # mark dataset as expired 12 hours from now; can be removed by CI
    expiration = int(
        ((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() + 60 * 60 * 12)
        * 1000
    )
    dataset.labels = {"expires_on": expiration}
    if suffix:
        dataset.labels["suffix"] = suffix
    if access_entries:
        dataset.access_entries = dataset.access_entries + access_entries

    return client.update_dataset(
        dataset, ["default_table_expiration_ms", "labels", "access_entries"]
    )


@stage.command(help="""
    Remove deployed artifacts from stage environment

    Coding agents aren't allowed to run this command.

    Examples:
    ./bqetl stage clean
    """)
@block_coding_agents
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project to deploy artifacts to",
    default="moz-fx-data-integration-tests",
)
@click.option(
    "--dataset-suffix",
    "--dataset_suffix",
    help="Suffix appended to the deployed dataset",
)
@click.option(
    "--delete-expired",
    "--delete_expired",
    help="Remove all datasets that have expired including those that do not match the suffix",
    default=False,
    is_flag=True,
)
def clean(project_id, dataset_suffix, delete_expired):
    """Reset the stage environment."""
    client = bigquery.Client(project_id)

    dataset_filter = (
        None if delete_expired is True else f"labels.suffix:{dataset_suffix}"
    )
    datasets = client.list_datasets(filter=dataset_filter)

    current_timestamp = int(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000
    )

    for dataset in datasets:
        if dataset.labels:
            # remove datasets that either match the suffix or are expired
            for label, value in dataset.labels.items():
                if (
                    dataset_suffix and label == "suffix" and value == dataset_suffix
                ) or (
                    delete_expired
                    and label == "expires_on"
                    and int(value) < current_timestamp
                ):
                    click.echo(f"Deleting dataset {dataset.full_dataset_id}")
                    try:
                        client.delete_dataset(dataset, delete_contents=True)
                    except NotFound as e:
                        # account for other concurrent CI runs
                        click.echo(f"Failed to delete: {e.message}")
