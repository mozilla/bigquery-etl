"""Represents a SQL view."""

import glob
import re
import string
import sys
import time
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional

import attr
import sqlparse
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from bigquery_etl.config import ConfigLoader
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metadata.parse_metadata import (
    DATASET_METADATA_FILE,
    METADATA_FILE,
    DatasetMetadata,
    Metadata,
)
from bigquery_etl.schema import SCHEMA_FILE, Schema
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.util.common import render

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
    partition_column: Optional[str] = attr.ib(None)
    id_token: Optional[Any] = attr.ib(None)

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
    def from_file(cls, path, **kwargs):
        """View from SQL file."""
        project, dataset, name = extract_from_query_path(path)
        return cls(
            path=str(path), name=name, dataset=dataset, project=project, **kwargs
        )

    @property
    def view_identifier(self):
        """Return full view identifier: `<project>.<dataset>.<name>`."""
        return f"{self.project}.{self.dataset}.{self.name}"

    @property
    def is_user_facing(self):
        """Return whether the view is user-facing."""
        return not self.dataset.endswith(
            tuple(
                ConfigLoader.get(
                    "default", "non_user_facing_dataset_suffixes", fallback=[]
                )
            )
        )

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

    def skip_validation(self):
        """Get views that should be skipped during validation."""
        return {
            file
            for skip in ConfigLoader.get("view", "validation", "skip", fallback=[])
            for file in glob.glob(
                skip,
                recursive=True,
            )
        }

    def skip_publish(self):
        """Get views that should be skipped during publishing."""
        return {
            file
            for skip in ConfigLoader.get("view", "publish", "skip", fallback=[])
            for file in glob.glob(
                skip,
                recursive=True,
            )
        }

    def is_valid(self) -> bool:
        """Validate the SQL view definition."""
        if any(str(self.path).endswith(p) for p in self.skip_validation()):
            print(f"Skipped validation for {self.path}")
            return True
        return self._valid_fully_qualified_references() and self._valid_view_naming()

    @cached_property
    def table_references(self):
        """List of table references in this view."""
        from bigquery_etl.dependency import extract_table_references

        return extract_table_references(self.content)

    @cached_property
    def udf_references(self):
        """List of UDF references in this view."""
        from bigquery_etl.routine.parse_routine import routine_usages_in_text

        # routine_usages_in_text automatically includes mozfun UDFs
        return routine_usages_in_text(
            self.content, Path(self.path).parent.parent.parent
        )

    @cached_property
    def schema(self):
        """Derive view schema from a schema file or a dry run result."""
        return self.configured_schema or self.dryrun_schema

    @cached_property
    def schema_path(self):
        """Return the schema file path."""
        return Path(self.path).parent / SCHEMA_FILE

    @cached_property
    def configured_schema(self):
        """Derive view schema from a schema file."""
        if self.schema_path.is_file():
            return Schema.from_schema_file(self.schema_path)
        return None

    @cached_property
    def dryrun_schema(self):
        """Derive view schema from a dry run result."""
        try:
            # We have to remove `CREATE OR REPLACE VIEW ... AS` from the query to avoid
            # view-creation-permission-denied errors, and we have to apply a `WHERE`
            # filter to avoid partition-column-filter-missing errors.
            schema_query_filter = (
                f"DATE(`{self.partition_column}`) = DATE('2020-01-01')"
                if self.partition_column
                else "FALSE"
            )
            schema_query = dedent(
                f"""
                WITH view_query AS (
                    {CREATE_VIEW_PATTERN.sub("", self.content)}
                )
                SELECT *
                FROM view_query
                WHERE {schema_query_filter}
                """
            )
            return Schema.from_query_file(
                Path(self.path), content=schema_query, id_token=self.id_token
            )
        except Exception as e:
            print(f"Error dry-running view {self.view_identifier} to get schema: {e}")
            return None

    def _valid_fully_qualified_references(self):
        """Check that referenced tables and views are fully qualified."""
        for table in self.table_references:
            if len(table.split(".")) < 3:
                print(f"{self.path} ERROR\n{table} missing project_id qualifier")
                return False
        return True

    def _valid_view_naming(self):
        """Validate that the created view naming matches the directory structure."""
        if not (parsed := sqlparse.parse(self.content)):
            raise ValueError(f"Unable to parse view SQL for {self.path}")

        tokens = [
            t
            for t in parsed[0].tokens
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

    def has_changes(self, target_project=None, credentials=None):
        """Determine whether there are any changes that would be published."""
        if any(str(self.path).endswith(p) for p in self.skip_publish()):
            return False

        if target_project and self.project != ConfigLoader.get(
            "default", "project", fallback="moz-fx-data-shared-prod"
        ):
            # view would be skipped because --target-project is set
            return False

        if credentials:
            client = bigquery.Client(credentials=credentials)
        else:
            client = bigquery.Client()
        target_view_id = self.target_view_identifier(target_project)
        try:
            table = client.get_table(target_view_id)
        except NotFound:
            print(f"view {target_view_id} will change: does not exist in BigQuery")
            return True

        try:
            expected_view_query = CREATE_VIEW_PATTERN.sub(
                "", sqlparse.format(self.content, strip_comments=True), count=1
            ).strip(";" + string.whitespace)

            actual_view_query = sqlparse.format(
                table.view_query, strip_comments=True
            ).strip(";" + string.whitespace)
        except TypeError:
            print(
                f"ERROR: There has been an issue formatting: {target_view_id}",
                file=sys.stderr,
            )
            raise

        if expected_view_query != actual_view_query:
            print(f"view {target_view_id} will change: query does not match")
            return True

        # check metadata
        if self.metadata is not None:
            if self.metadata.description != table.description:
                print(f"view {target_view_id} will change: description does not match")
                return True
            if self.metadata.friendly_name != table.friendly_name:
                print(
                    f"view {target_view_id} will change: friendly_name does not match"
                )
                return True
            if self.labels != table.labels:
                print(f"view {target_view_id} will change: labels do not match")
                return True

        table_schema = Schema.from_bigquery_schema(table.schema)

        if self.schema is not None and not self.schema.equal(table_schema):
            print(f"view {target_view_id} will change: schema does not match")
            return True

        return False

    def publish(self, target_project=None, dry_run=False, client=None):
        """
        Publish this view to BigQuery.

        If `target_project` is set, it will replace the project ID in the view definition.
        """
        if any(str(self.path).endswith(p) for p in self.skip_publish()):
            print(f"Skipping {self.path}")
            return True

        # avoid checking references since Jenkins might throw an exception:
        # https://github.com/mozilla/bigquery-etl/issues/2246
        if (
            any(str(self.path).endswith(p) for p in self.skip_validation())
            or self._valid_view_naming()
        ):
            client = client or bigquery.Client()
            sql = self.content
            target_view = self.target_view_identifier(target_project)

            if target_project:
                if self.project != ConfigLoader.get(
                    "default", "project", fallback="moz-fx-data-shared-prod"
                ):
                    print(f"Skipping {self.path} because --target-project is set")
                    return True

                # We only change the first occurrence, which is in the target view name.
                sql = re.sub(
                    rf"^(?!--)(.*){self.project}",
                    rf"\1{target_project}",
                    sql,
                    count=1,
                    flags=re.MULTILINE,
                )

            job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dry_run)
            query_job = client.query(sql, job_config)

            if dry_run:
                print(f"Validated definition of {target_view} in {self.path}")
            else:
                try:
                    job_id = query_job.result().job_id
                except BadRequest as e:
                    if "Invalid snapshot time" in e.message:
                        # This occasionally happens due to dependent views being
                        # published concurrently; we wait briefly and give it one
                        # extra try in this situation.
                        time.sleep(1)
                        job_id = client.query(sql, job_config).result().job_id
                    else:
                        raise

                try:
                    table = client.get_table(target_view)
                except NotFound:
                    print(
                        f"{target_view} failed to publish to the correct location, verify job id {job_id}",
                        file=sys.stderr,
                    )
                    return False

                try:
                    if self.schema_path.is_file():
                        table = self.schema.deploy(target_view)
                except Exception as e:
                    print(f"Could not update field descriptions for {target_view}: {e}")

                if not self.metadata:
                    print(f"Missing metadata for {self.path}")

                table.description = self.metadata.description
                table.friendly_name = self.metadata.friendly_name

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
                    client.update_table(
                        table, ["labels", "description", "friendly_name"]
                    )
                else:
                    client.update_table(table, ["description", "friendly_name"])

                print(f"Published view {target_view}")
        else:
            print(
                f"Error publishing {self.path}. Invalid view definition.",
                file=sys.stderr,
            )
            return False

        return True
