"""Represents a SQL view."""

import attr
import sqlparse

from google.cloud import bigquery
from pathlib import Path

from bigquery_etl.util import extract_from_query_path


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
}

# suffixes of datasets with non-user-facing views
NON_USER_FACING_DATASET_SUFFIXES = (
    "_derived",
    "_external",
    "_bi",
    "_restricted",
)


@attr.s(auto_attribs=True)
class View:
    """Representation of a SQL view stored in a view.sql file."""

    path: str = attr.ib()
    name: str = attr.ib()
    dataset: str = attr.ib()
    project: str = attr.ib()

    # todo: validators

    def content(self):
        """Return the view SQL."""
        return Path(self.path).read_text()

    @classmethod
    def from_file(cls, path):
        """View from SQL file."""
        project, dataset, name = extract_from_query_path(path)
        return cls(path=str(path), name=name, dataset=dataset, project=project)

    def is_valid(self):
        """Validate the SQL view definition."""
        if self.path in SKIP_VALIDATION:
            print(f"Skipped validation for {self.path}")
            return True
        return self._valid_fully_qualified_references() and self._valid_view_naming()

    def _valid_fully_qualified_references(self):
        """Check that referenced tables and views are fully qualified."""
        from bigquery_etl.dependency import extract_table_references

        for table in extract_table_references(self.content()):
            if len(table.split(".")) < 3:
                print(f"{self.path} ERROR\n{table} missing project_id qualifier")
                return False
        return True

    def _valid_view_naming(self):
        """Validate that the created view naming matches the directory structure."""
        parsed = sqlparse.parse(self.content())[0]
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

    def publish(self):
        """Publish this view to BigQuery."""
        client = bigquery.Client()