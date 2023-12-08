"""Parsing of metadata yaml files."""

import enum
import os
import re
import string
from pathlib import Path
from typing import Any, Dict, List, Optional

import attr
import cattrs
import yaml
from google.cloud import bigquery

from bigquery_etl.query_scheduling.utils import is_email, is_email_or_github_identity

METADATA_FILE = "metadata.yaml"
DATASET_METADATA_FILE = "dataset_metadata.yaml"
DEFAULT_WORKGROUP_ACCESS = [
    dict(role="roles/bigquery.dataViewer", members=["workgroup:mozilla-confidential"])
]
DEFAULT_TABLE_WORKGROUP_ACCESS = DEFAULT_WORKGROUP_ACCESS


class Literal(str):
    """Represents a YAML literal."""

    pass


def literal_presenter(dumper, data):
    """Literal representer for YAML output."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(Literal, literal_presenter)


class PartitionType(enum.Enum):
    """Represents BigQuery table partition types."""

    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    @property
    def bigquery_type(self):
        """Map to the BigQuery data type."""
        d = {
            "hour": bigquery.TimePartitioningType.HOUR,
            "day": bigquery.TimePartitioningType.DAY,
            "month": bigquery.TimePartitioningType.MONTH,
            "year": bigquery.TimePartitioningType.YEAR,
        }
        return d[self.value]


@attr.s(auto_attribs=True)
class PartitionMetadata:
    """Metadata for defining BigQuery table partitions."""

    type: PartitionType
    field: Optional[str] = attr.ib(None)
    require_partition_filter: bool = attr.ib(True)
    expiration_days: Optional[float] = attr.ib(None)

    @property
    def expiration_ms(self) -> Optional[float]:
        """Convert partition expiration from days to milliseconds."""
        if self.expiration_days is None:
            return None

        return int(self.expiration_days * 86400000)


@attr.s(auto_attribs=True)
class ClusteringMetadata:
    """Metadata for defining BigQuery table clustering."""

    fields: List[str]


@attr.s(auto_attribs=True)
class BigQueryMetadata:
    """
    Metadata related to BigQuery configurations for the query.

    For example, partitioning or clustering of the destination table.
    """

    time_partitioning: Optional[PartitionMetadata] = attr.ib(None)
    clustering: Optional[ClusteringMetadata] = attr.ib(None)


@attr.s(auto_attribs=True)
class SchemaDerivedMetadata:
    """Metadata specifying parent schema."""

    table: List[str]
    # list of excluded columns
    exclude: Optional[List[str]] = attr.ib(None)


@attr.s(auto_attribs=True)
class SchemaMetadata:
    """Metadata related to additional schema information."""

    derived_from: List[SchemaDerivedMetadata]


@attr.s(auto_attribs=True)
class WorkgroupAccessMetadata:
    """Workgroup access metadata."""

    role: str
    members: List[str]


class ExternalDataFormat(enum.Enum):
    """Represents the external types fo data that are supported to be integrated."""

    GOOGLE_SHEETS = "google_sheets"
    CSV = "csv"


@attr.s(auto_attribs=True)
class ExternalDataMetadata:
    """Metadata for specifying external data."""

    format: ExternalDataFormat
    source_uris: List[str]
    options: Optional[Dict[str, Any]] = attr.ib(None)


class CheckStatus(enum.Enum):
    """ETL Check status for which to send slack notification."""

    FAIL = "fail"
    WARN = "warn"
    SUCCESS = "success"


@attr.s(auto_attribs=True)
class SlackNotificationMetadata:
    """Metadata for configuring slack notifications for checks."""

    status: List[CheckStatus]
    channel: str


@attr.s(auto_attribs=True)
class ChecksMetadata:
    """Metadata for configuring ETL checks."""

    slack_notification: SlackNotificationMetadata


@attr.s(auto_attribs=True)
class Metadata:
    """
    Representation of a table or view Metadata configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    friendly_name: str = attr.ib()
    description: str = attr.ib()
    owners: List[str] = attr.ib()
    labels: Dict = attr.ib({})
    scheduling: Optional[Dict] = attr.ib({})
    bigquery: Optional[BigQueryMetadata] = attr.ib(None)
    schema: Optional[SchemaMetadata] = attr.ib(None)
    workgroup_access: Optional[List[WorkgroupAccessMetadata]] = attr.ib(None)
    references: Dict = attr.ib({})
    external_data: Optional[ExternalDataMetadata] = attr.ib(None)
    deprecated: bool = attr.ib(False)
    checks: Optional[ChecksMetadata] = attr.ib(None)

    @owners.validator
    def validate_owners(self, attribute, value):
        """Check that provided email addresses or github identities for owners are valid."""
        if not all(map(lambda e: is_email_or_github_identity(e), value)):
            raise ValueError(f"Invalid email or Github identity for owners: {value}.")

    @labels.validator
    def validate_labels(self, attribute, value):
        """Check that labels are valid."""
        for key, label in value.items():
            if key == "review_bugs" and label != "":
                if isinstance(label, list):
                    for bug in label:
                        if not Metadata.is_valid_label(str(bug)):
                            raise ValueError(f"Invalid label format: {bug}")
                else:
                    raise ValueError("Error: review_bugs needs to be a list.")
            elif not isinstance(label, bool):
                if not Metadata.is_valid_label(str(key)):
                    raise ValueError(f"Invalid label format: {key}")
                elif not Metadata.is_valid_label(str(label)) and label != "":
                    raise ValueError(f"Invalid label format: {label}")

    @staticmethod
    def is_valid_label(label):
        """
        Check if a label has the right format.

        Only hyphens (-), underscores (_), lowercase characters, and
        numbers are allowed. International characters are not allowed.

        Keys have a minimum length of 1 character and a maximum length of
        63 characters, and cannot be empty. Values can be empty, and have
        a maximum length of 63 characters.
        """
        return re.fullmatch(r"[0-9a-z-_]{1,63}", label) is not None

    @staticmethod
    def is_metadata_file(file_path):
        """
        Check if the provided file is a metadata file.

        Checks if the name and file format match the metadata file requirements.
        """
        # todo: we should probably also check if the file actually exists etc.
        return os.path.basename(file_path) == METADATA_FILE

    @classmethod
    def of_table(cls, dataset, table, version, target_dir):
        """
        Return metadata that is associated with the provided table.

        The provided directory is searched for metadata files and is expected to
        have the following structure: /<dataset>/<table>_<version>/metadata.yaml.
        """
        path = os.path.join(target_dir, dataset, table + "_" + version)
        metadata_file = os.path.join(path, METADATA_FILE)
        cls = Metadata.from_file(metadata_file)
        return cls

    @classmethod
    def from_file(cls, metadata_file):
        """Parse metadata from the provided file and create a new Metadata instance."""
        friendly_name = None
        description = None
        owners = []
        labels = {}
        scheduling = {}
        bigquery = None
        schema = None
        workgroup_access = None
        references = {}
        external_data = None
        deprecated = False
        checks = None

        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)
                table_name = str(Path(metadata_file).parent.name)
                friendly_name = metadata.get(
                    "friendly_name", string.capwords(table_name.replace("_", " "))
                )
                description = metadata.get(
                    "description",
                    "Please provide a description for the query",
                )

                if "labels" in metadata:
                    for key, label in metadata["labels"].items():
                        if isinstance(label, bool):
                            # publish key-value pair with bool value as tag
                            if label:
                                labels[str(key)] = ""
                        elif isinstance(label, list):
                            labels[str(key)] = list(map(str, label))
                        else:
                            # all other pairs get published as key-value pair label
                            labels[str(key)] = str(label)

                if "scheduling" in metadata:
                    scheduling = metadata["scheduling"]
                    if "dag_name" in scheduling and cls.is_valid_label(
                        scheduling["dag_name"]
                    ):
                        labels["dag"] = scheduling["dag_name"]

                if "bigquery" in metadata and metadata["bigquery"]:
                    converter = cattrs.BaseConverter()
                    bigquery = converter.structure(
                        metadata["bigquery"], BigQueryMetadata
                    )

                if "owners" in metadata:
                    owners = metadata["owners"]
                    owner_idx = 1
                    for owner in filter(is_email, owners):
                        label = owner.split("@")[0]
                        if Metadata.is_valid_label(label):
                            labels[f"owner{owner_idx}"] = label
                            owner_idx += 1

                if "schema" in metadata:
                    converter = cattrs.BaseConverter()
                    schema = converter.structure(metadata["schema"], SchemaMetadata)

                if "workgroup_access" in metadata:
                    converter = cattrs.BaseConverter()
                    workgroup_access = converter.structure(
                        metadata["workgroup_access"], List[WorkgroupAccessMetadata]
                    )

                if "references" in metadata:
                    references = metadata["references"]

                if "external_data" in metadata:
                    converter = cattrs.BaseConverter()
                    external_data = converter.structure(
                        metadata["external_data"], ExternalDataMetadata
                    )
                if "deprecated" in metadata:
                    deprecated = metadata["deprecated"]

                if "checks" in metadata:
                    converter = cattrs.BaseConverter()
                    external_data = converter.structure(
                        metadata["checks"], ChecksMetadata
                    )

                return cls(
                    friendly_name,
                    description,
                    owners,
                    labels,
                    scheduling,
                    bigquery,
                    schema,
                    workgroup_access,
                    references,
                    external_data,
                    deprecated,
                    checks,
                )
            except yaml.YAMLError as e:
                raise e

    @classmethod
    def of_query_file(cls, sql_file):
        """Return the metadata that is associated with the provided SQL file."""
        path, _ = os.path.split(sql_file)
        metadata_file = os.path.join(path, METADATA_FILE)
        cls = Metadata.from_file(metadata_file)
        return cls

    def write(self, file):
        """Write metadata information to the provided file."""
        converter = cattrs.BaseConverter()
        metadata_dict = converter.unstructure(self)

        if metadata_dict["scheduling"] == {}:
            del metadata_dict["scheduling"]

        if metadata_dict["labels"]:
            for label_key, label_value in metadata_dict["labels"].items():
                # handle tags
                if label_value == "":
                    metadata_dict["labels"][label_key] = True

        if "description" in metadata_dict:
            metadata_dict["description"] = Literal(metadata_dict["description"])

        if metadata_dict["schema"] is None:
            del metadata_dict["schema"]

        if metadata_dict["workgroup_access"] is None:
            del metadata_dict["workgroup_access"]

        if metadata_dict["external_data"] is None:
            del metadata_dict["external_data"]

        if metadata_dict["checks"] is None:
            del metadata_dict["checks"]

        file.write_text(
            yaml.dump(
                converter.unstructure(metadata_dict),
                default_flow_style=False,
                sort_keys=False,
            )
        )

    def is_public_bigquery(self):
        """Return true if the public_bigquery flag is set."""
        return "public_bigquery" in self.labels

    def is_public_json(self):
        """Return true if the public_json flag is set."""
        return "public_json" in self.labels

    def is_incremental(self):
        """Return true if the incremental flag is set."""
        return "incremental" in self.labels

    def is_incremental_export(self):
        """Return true if the incremental_export flag is set."""
        return "incremental_export" in self.labels

    def review_bugs(self):
        """Return the bug ID of the data review bug in bugzilla."""
        return self.labels.get("review_bugs", None)

    def set_bigquery_partitioning(
        self, field, partition_type, required, expiration_days=None
    ):
        """Update the BigQuery partitioning metadata."""
        clustering = None
        if self.bigquery and self.bigquery.clustering:
            clustering = self.bigquery.clustering

        self.bigquery = BigQueryMetadata(
            time_partitioning=PartitionMetadata(
                field=field,
                type=PartitionType(partition_type),
                require_partition_filter=required,
                expiration_days=expiration_days,
            ),
            clustering=clustering,
        )

    def set_bigquery_clustering(self, clustering_fields):
        """Update the BigQuery partitioning metadata."""
        partitioning = None
        if self.bigquery and self.bigquery.time_partitioning:
            partitioning = self.bigquery.time_partitioning

        self.bigquery = BigQueryMetadata(
            time_partitioning=partitioning,
            clustering=ClusteringMetadata(fields=clustering_fields),
        )


@attr.s(auto_attribs=True)
class DatasetMetadata:
    """
    Representation of a dataset-level metadata configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    friendly_name: str = attr.ib()
    description: str = attr.ib()
    dataset_base_acl: str = attr.ib()
    user_facing: bool = attr.ib(False)
    labels: Dict = attr.ib({})
    default_table_workgroup_access: Optional[List[Dict[str, Any]]] = attr.ib(None)
    workgroup_access: list = attr.ib(DEFAULT_WORKGROUP_ACCESS)

    def __attrs_post_init__(self):
        """Set default table workgroup access to workgroup access."""
        if self.default_table_workgroup_access is None:
            self.default_table_workgroup_access = self.workgroup_access

    @staticmethod
    def is_dataset_metadata_file(file_path):
        """
        Check if the provided file is a metadata file.

        Checks if the name and file format match the metadata file requirements.
        """
        # todo: we should probably also check if the file actually exists etc.
        return os.path.basename(file_path) == DATASET_METADATA_FILE

    def write(self, file):
        """Write dataset metadata information to the provided file."""
        metadata_dict = self.__dict__
        if metadata_dict["labels"]:
            for label_key, label_value in metadata_dict["labels"].items():
                # handle tags
                if label_value == "":
                    metadata_dict["labels"][label_key] = True

        if "description" in metadata_dict:
            metadata_dict["description"] = Literal(metadata_dict["description"])

        if "default_table_workgroup_access" in metadata_dict:
            metadata_dict["default_table_workgroup_access"] = metadata_dict[
                "default_table_workgroup_access"
            ]

        converter = cattrs.BaseConverter()
        file.write_text(
            yaml.dump(
                converter.unstructure(metadata_dict),
                default_flow_style=False,
                sort_keys=False,
            )
        )

    @classmethod
    def from_file(cls, metadata_file):
        """Parse dataset metadata from the provided file.

        Returns a new DatasetMetadata instance.
        """
        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)
                return cls(**metadata)
            except yaml.YAMLError as e:
                raise e
