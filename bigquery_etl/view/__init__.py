"""Represents a SQL view."""

import attr
import sqlparse
import string
import time

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from pathlib import Path

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metadata.parse_metadata import DatasetMetadata, DATASET_METADATA_FILE
from bigquery_etl.util import extract_from_query_path
from bigquery_etl.schema import Schema


# skip validation for these views
SKIP_VALIDATION = {
    # not matching directory structure, but created before validation was enforced
    "sql/moz-fx-data-shared-prod/stripe/subscription/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/product/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/plan/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/client_probe_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_histogram_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_histogram_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_probe_processes/view.sql",
    # tests
    "sql/moz-fx-data-test-project/test/simple_view/view.sql",
    # Access Denied
    "sql/moz-fx-data-shared-prod/telemetry/experiment_enrollment_cumulative_population_estimate/view.sql",  # noqa E501
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
        return Path(self.path).read_text()

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

    def _valid_fully_qualified_references(self):
        """Check that referenced tables and views are fully qualified."""
        from bigquery_etl.dependency import extract_table_references

        for table in extract_table_references(self.content):
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
            target_view = self.view_identifier

            if target_project:
                if self.project != "moz-fx-data-shared-prod":
                    print(f"Skipping {self.path} because --target-project is set")
                    return True

                # target_view must be a fully-qualified BigQuery Standard SQL table
                # identifier, which is of the form f"{project_id}.{dataset_id}.{table_id}".
                # dataset_id and table_id may not contain "." or "`". Each component may be
                # a backtick (`) quoted identifier, or the whole thing may be a backtick
                # quoted identifier, but not both.
                # Project IDs must contain 6-63 lowercase letters, digits, or dashes. Some
                # project IDs also include domain name separated by a colon. IDs must start
                # with a letter and may not end with a dash. For more information see also
                # https://github.com/mozilla/bigquery-etl/pull/1427#issuecomment-707376291
                target_view = self.view_identifier.replace(
                    self.project, target_project, 1
                )
                # We only change the first occurrence, which is in the target view name.
                sql = sql.replace(self.project, target_project, 1)

            job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dry_run)
            query_job = client.query(sql, job_config)

            if dry_run:
                print(f"Validated definition of {self.view_identifier} in {self.path}")
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
                    view_schema = Schema.from_schema_file(
                        Path(self.path).parent / "schema.yaml"
                    )
                    view_schema.deploy(target_view)
                except Exception as e:
                    print(f"Could not update field descriptions for {target_view}: {e}")
                print(f"Published view {target_view}")
        else:
            print(f"Error publishing {self.path}. Invalid view definition.")
            return False

        return True
