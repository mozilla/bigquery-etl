"""Represents a SQL view."""

import glob
import re
import string
import time
from functools import cached_property
from pathlib import Path

import attr
import sqlparse
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metadata.parse_metadata import (
    DATASET_METADATA_FILE,
    METADATA_FILE,
    DatasetMetadata,
    Metadata,
)
from bigquery_etl.schema import Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.common import render

# skip validation for these views
SKIP_VALIDATION = {
    # tests
    "sql/moz-fx-data-test-project/test/simple_view/view.sql",
    # Access Denied
    "sql/moz-fx-data-shared-prod/telemetry/experiment_enrollment_cumulative_population_estimate/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/mlhackweek_search/events/view.sql",
    "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/deletion_request/view.sql",
    "sql/moz-fx-data-shared-prod/mlhackweek_search/custom/view.sql",
    "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/regret_details/view.sql",
    "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/video_data/view.sql",
    "sql/moz-fx-data-shared-prod/mlhackweek_search/deletion_request/view.sql",
    "sql/moz-fx-data-shared-prod/mlhackweek_search/baseline/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/regrets_reporter_update/view.sql",
    "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/video_index/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/xfocsp_error_report/view.sql",
    "sql/moz-fx-data-shared-prod/mlhackweek_search/metrics/view.sql",
    "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/main_events/view.sql",
    "sql/moz-fx-data-shared-prod/mlhackweek_search/action/view.sql",
    *glob.glob(
        "sql/moz-fx-data-shared-prod/**/client_deduplication/view.sql",
        recursive=True,
    ),
}

# skip publishing these views
SKIP_PUBLISHING = {
    # Access Denied
    "activity_stream/tile_id_types/view.sql",
    "pocket/pocket_reach_mau/view.sql",
    "telemetry/buildhub2/view.sql",
    # Dataset glam-fenix-dev:glam_etl was not found
    # TODO: this should be removed if views are to be automatically deployed
    *[str(path) for path in Path("sql/glam-fenix-dev").glob("glam_etl/**/view.sql")],
    # tests
    "sql/moz-fx-data-test-project/test/simple_view/view.sql",
}

# suffixes of datasets with non-user-facing views
NON_USER_FACING_DATASET_SUFFIXES = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
    "glam_etl",
)

# Regex matching CREATE VIEW statement so it can be removed to get the view query
CREATE_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+[^\s]+\s+AS", re.IGNORECASE
)


@attr.s(auto_attribs=True)
class View:
    """Representation of a SQL view stored in a view.sql file."""

    path: str = attr.ib()
    name: str = attr.ib()
    dataset: str = attr.ib()
    project: str = attr.ib()

    @path.validator
    def validate_path(self, attribute, value):
        """Check that the view path is valid."""
        if not Path(self.path).exists():
            raise ValueError(f"View file does not exist: {self.path}")

    @property
    def content(self):
        """Return the view SQL."""
        path = Path(self.path)
        return render(path.name, template_folder=path.parent)

    @classmethod
    def from_file(cls, path):
        """View from SQL file."""
        project, dataset, name = extract_from_query_path(path)
        return cls(path=str(path), name=name, dataset=dataset, project=project)

    @property
    def view_identifier(self):
        """Return full view identifier: `<project>.<dataset>.<name>`."""
        return f"{self.project}.{self.dataset}.{self.name}"

    @property
    def is_user_facing(self):
        """Return whether the view is user-facing."""
        return not self.dataset.endswith(NON_USER_FACING_DATASET_SUFFIXES)

    @cached_property
    def metadata(self):
        """Return the view metadata."""
        path = Path(self.path).parent / METADATA_FILE
        if not path.exists():
            return None
        return Metadata.from_file(path)

    @property
    def labels(self):
        """Return the view labels."""
        if not hasattr(self, "_labels"):
            if self.metadata:
                self._labels = self.metadata.labels.copy()
            else:
                self._labels = {}
        return self._labels

    @classmethod
    def create(cls, project, dataset, name, sql_dir, base_table=None):
        """
        Create a new empty view from a template.

        Use `base_table` in view definition, if provided.
        """
        path = Path(sql_dir) / project / dataset / name / "view.sql"
        dataset_path = path.parent.parent

        if not dataset_path.exists():
            # create new dataset with dataset metadata
            path.parent.mkdir(parents=True)
            dataset_metadata = DatasetMetadata(
                friendly_name=string.capwords(dataset),
                description="Please provide a dataset description.",
                dataset_base_acl="view",
                user_facing=True,
            )
            dataset_metadata.write(dataset_path / DATASET_METADATA_FILE)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)

        if not base_table:
            base_table = f"{project}.{dataset}_derived.{name}_v1"

        path.write_text(
            reformat(
                f"""
                CREATE OR REPLACE VIEW `{project}.{dataset}.{name}` AS
                SELECT * FROM `{base_table}`
                """
            )
            + "\n"
        )
        return cls(path, name, dataset, project)

    def is_valid(self):
        """Validate the SQL view definition."""
        if any(str(self.path).endswith(p) for p in SKIP_VALIDATION):
            print(f"Skipped validation for {self.path}")
            return True
        return self._valid_fully_qualified_references() and self._valid_view_naming()

    @cached_property
    def table_references(self):
        """List of table references in this view."""
        from bigquery_etl.dependency import extract_table_references

        return extract_table_references(self.content)

    def _valid_fully_qualified_references(self):
        """Check that referenced tables and views are fully qualified."""
        for table in self.table_references:
            if len(table.split(".")) < 3:
                print(f"{self.path} ERROR\n{table} missing project_id qualifier")
                return False
        return True

    def _valid_view_naming(self):
        """Validate that the created view naming matches the directory structure."""
        parsed = sqlparse.parse(self.content)[0]
        tokens = [
            t
            for t in parsed.tokens
            if not (t.is_whitespace or isinstance(t, sqlparse.sql.Comment))
        ]
        is_view_statement = (
            " ".join(tokens[0].normalized.split()) == "CREATE OR REPLACE"
            and tokens[1].normalized == "VIEW"
        )
        if is_view_statement:
            target_view = str(tokens[2]).strip().split()[0]
            try:
                [project_id, dataset_id, view_id] = target_view.replace("`", "").split(
                    "."
                )
                if not (
                    self.name == view_id
                    and self.dataset == dataset_id
                    and self.project == project_id
                ):
                    print(
                        f"{self.path} ERROR\n"
                        f"View name {target_view} not matching directory structure."
                    )
                    return False
            except Exception:
                print(f"{self.path} ERROR\n{target_view} missing project ID qualifier.")
                return False
        else:
            print(
                f"ERROR: {self.path} does not appear to be "
                "a CREATE OR REPLACE VIEW statement! Quitting..."
            )
            return False
        return True

    def target_view_identifier(self, target_project=None):
        """Return the view identifier after replacing project with target_project.

        Result must be a fully-qualified BigQuery Standard SQL table identifier, which
        is of the form f"{project_id}.{dataset_id}.{table_id}". dataset_id and table_id
        may not contain "." or "`". Each component may be a backtick (`) quoted
        identifier, or the whole thing may be a backtick quoted identifier, but not
        both. Project IDs must contain 6-63 lowercase letters, digits, or dashes. Some
        project IDs also include domain name separated by a colon. IDs must start with a
        letter and may not end with a dash. For more information see also
        https://github.com/mozilla/bigquery-etl/pull/1427#issuecomment-707376291
        """
        if target_project:
            return self.view_identifier.replace(self.project, target_project, 1)
        return self.view_identifier

    def has_changes(self, target_project=None):
        """Determine whether there are any changes that would be published."""
        if any(str(self.path).endswith(p) for p in SKIP_PUBLISHING):
            return False

        if target_project and self.project != "moz-fx-data-shared-prod":
            # view would be skipped because --target-project is set
            return False

        client = bigquery.Client()
        target_view_id = self.target_view_identifier(target_project)
        try:
            table = client.get_table(target_view_id)
        except NotFound:
            print(f"view {target_view_id} will change: does not exist in BigQuery")
            return True

        expected_view_query = CREATE_VIEW_PATTERN.sub(
            "", sqlparse.format(self.content, strip_comments=True), count=1
        ).strip(";" + string.whitespace)
        actual_view_query = sqlparse.format(
            table.view_query, strip_comments=True
        ).strip(";" + string.whitespace)
        if expected_view_query != actual_view_query:
            print(f"view {target_view_id} will change: query does not match")
            return True

        # check schema
        schema_file = Path(self.path).parent / "schema.yaml"
        if schema_file.is_file():
            view_schema = Schema.from_schema_file(schema_file)
            table_schema = Schema.from_json(
                {"fields": [f.to_api_repr() for f in table.schema]}
            )
            if not view_schema.equal(table_schema):
                print(f"view {target_view_id} will change: schema does not match")
                return True

        if self.labels != table.labels:
            print(f"view {target_view_id} will change: labels do not match")
            return True
        return False

    def publish(self, target_project=None, dry_run=False):
        """
        Publish this view to BigQuery.

        If `target_project` is set, it will replace the project ID in the view definition.
        """
        if any(str(self.path).endswith(p) for p in SKIP_PUBLISHING):
            print(f"Skipping {self.path}")
            return True

        # avoid checking references since Jenkins might throw an exception:
        # https://github.com/mozilla/bigquery-etl/issues/2246
        if (
            any(str(self.path).endswith(p) for p in SKIP_VALIDATION)
            or self._valid_view_naming()
        ):
            client = bigquery.Client()
            sql = self.content
            target_view = self.target_view_identifier(target_project)

            if target_project:
                if self.project != "moz-fx-data-shared-prod":
                    print(f"Skipping {self.path} because --target-project is set")
                    return True

                # We only change the first occurrence, which is in the target view name.
                sql = sql.replace(self.project, target_project, 1)

            job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dry_run)
            query_job = client.query(sql, job_config)

            if dry_run:
                print(f"Validated definition of {target_view} in {self.path}")
            else:
                try:
                    query_job.result()
                except BadRequest as e:
                    if "Invalid snapshot time" in e.message:
                        # This occasionally happens due to dependent views being
                        # published concurrently; we wait briefly and give it one
                        # extra try in this situation.
                        time.sleep(1)
                        client.query(sql, job_config).result()
                    else:
                        raise

                try:
                    schema_path = Path(self.path).parent / "schema.yaml"
                    if schema_path.is_file():
                        view_schema = Schema.from_schema_file(schema_path)
                        view_schema.deploy(target_view)
                except Exception as e:
                    print(f"Could not update field descriptions for {target_view}: {e}")

                table = client.get_table(target_view)
                if table.labels != self.labels:
                    labels = self.labels.copy()
                    for key in table.labels:
                        if key not in labels:
                            # To delete a label its value must be set to None
                            labels[key] = None
                    table.labels = {
                        key: value
                        for key, value in labels.items()
                        if isinstance(value, str)
                    }
                    client.update_table(table, ["labels"])

                print(f"Published view {target_view}")
        else:
            print(f"Error publishing {self.path}. Invalid view definition.")
            return False

        return True
