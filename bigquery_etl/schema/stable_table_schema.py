"""Methods for working with stable table schemas."""
import json
import tarfile
import urllib.request
from dataclasses import dataclass
from io import BytesIO
from itertools import groupby
from typing import List

from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import DryRun


@dataclass
class SchemaFile:
    """Container for metadata about a JSON schema and corresponding BQ table."""

    schema: dict
    schema_id: str
    bq_dataset_family: str
    bq_table: str
    document_namespace: str
    document_type: str
    document_version: int

    @property
    def bq_table_unversioned(self):
        """Return table_id with version suffix stripped."""
        return "_".join(self.bq_table.split("_")[:-1])

    @property
    def stable_table(self):
        """Return BQ stable table name in <dataset>.<table> form."""
        return f"{self.bq_dataset_family}_stable.{self.bq_table}"

    @property
    def user_facing_view(self):
        """Return user-facing view name in <dataset>.<view> form."""
        return f"{self.bq_dataset_family}.{self.bq_table_unversioned}"

    @property
    def sortkey(self):
        """Return variant of stable_table with zero-padded version for sorting."""
        return (
            "_".join(self.stable_table.split("_")[:-1]) + f"{self.document_version:04d}"
        )


def prod_schemas_uri():
    """Return URI for the schemas tarball deployed to shared-prod.

    We construct a fake query and send it to the dry run service in order
    to read dataset labels, which contains the commit hash associated
    with the most recent production schemas deploy.
    """
    dryrun = DryRun("telemetry_derived/foo/query.sql", content="SELECT 1")
    build_id = dryrun.get_dataset_labels()["schemas_build_id"]
    commit_hash = build_id.split("_")[-1]
    mps_uri = ConfigLoader.get("schema", "mozilla_pipeline_schemas_uri")
    return f"{mps_uri}/archive/{commit_hash}.tar.gz"


def get_stable_table_schemas() -> List[SchemaFile]:
    """Fetch last schema metadata per doctype by version."""
    schemas_uri = prod_schemas_uri()
    with urllib.request.urlopen(schemas_uri) as f:
        tarbytes = BytesIO(f.read())

    schemas = []
    with tarfile.open(fileobj=tarbytes, mode="r:gz") as tar:
        for tarinfo in tar:
            if tarinfo.name.endswith(".schema.json"):
                *_, document_namespace, document_type, basename = tarinfo.name.split(
                    "/"
                )
                version = int(basename.split(".")[1])
                schema = json.load(tar.extractfile(tarinfo.name))  # type: ignore
                bq_schema = {}

                # Schemas without pipeline metadata (like glean/glean)
                # do not have corresponding BQ tables, so we skip them here.
                pipeline_meta = schema.get("mozPipelineMetadata", None)
                if pipeline_meta is None:
                    continue

                try:
                    bq_schema_file = tar.extractfile(
                        tarinfo.name.replace(".schema.json", ".bq")
                    )
                    bq_schema = json.load(bq_schema_file)  # type: ignore
                except KeyError as e:
                    print(f"Cannot get Bigquery schema for {tarinfo.name}: {e}")

                schemas.append(
                    SchemaFile(
                        schema=bq_schema,
                        schema_id=schema.get("$id", ""),
                        bq_dataset_family=pipeline_meta["bq_dataset_family"],
                        bq_table=pipeline_meta["bq_table"],
                        document_namespace=document_namespace,
                        document_type=document_type,
                        document_version=version,
                    )
                )

    # Exclude doctypes maintained in separate projects.
    for prefix in ConfigLoader.get("schema", "skip_prefixes", fallback=[]):
        schemas = [
            schema
            for schema in schemas
            if not schema.document_namespace.startswith(prefix)
        ]

    # Retain only the highest version per doctype.
    schemas = sorted(
        schemas,
        key=lambda t: f"{t.document_namespace}/{t.document_type}/{t.document_version:03d}",
    )
    schemas = [
        last
        for k, (*_, last) in groupby(
            schemas, lambda t: f"{t.document_namespace}/{t.document_type}"
        )
    ]

    return schemas
