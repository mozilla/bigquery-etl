"""Parsing of metadata yaml files."""

import enum
import os
import re
from typing import Dict, List, Optional

import attr
import cattr
import yaml
from google.cloud import bigquery

from bigquery_etl.query_scheduling.utils import is_email

METADATA_FILE = "metadata.yaml"


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

    field: str
    type: PartitionType
    require_partition_filter: bool = attr.ib(False)


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
class Metadata:
    """
    Representation of a Metadata configuration.

    Uses attrs to simplify the class definition and provide validation.
    Docs: https://www.attrs.org
    """

    friendly_name: str = attr.ib()
    description: str = attr.ib()
    owners: List[str] = attr.ib()
    labels: Dict = attr.ib({})
    scheduling: Optional[Dict] = attr.ib({})
    bigquery: Optional[BigQueryMetadata] = attr.ib(None)

    @owners.validator
    def validate_owners(self, attribute, value):
        """Check that provided email addresses for owners are valid."""
        if not all(map(lambda e: is_email(e), value)):
            raise ValueError(f"Invalid email for owners: {value}.")

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

        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)

                friendly_name = metadata.get("friendly_name", None)
                description = metadata.get("description", None)

                if "labels" in metadata:
                    labels = {}

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

                if "bigquery" in metadata and metadata["bigquery"]:
                    converter = cattr.Converter()
                    bigquery = converter.structure(
                        metadata["bigquery"], BigQueryMetadata
                    )

                if "owners" in metadata:
                    owners = metadata["owners"]

                return cls(
                    friendly_name, description, owners, labels, scheduling, bigquery
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
        metadata_dict = self.__dict__
        if metadata_dict["scheduling"] == {}:
            del metadata_dict["scheduling"]

        if metadata_dict["labels"]:
            for label_key, label_value in metadata_dict["labels"].items():
                # handle tags
                if label_value == "":
                    metadata_dict["labels"][label_key] = True

        if "description" in metadata_dict:
            metadata_dict["description"] = Literal(metadata_dict["description"])

        converter = cattr.Converter()
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

    def set_bigquery_partitioning(self, field, partition_type, required):
        """Update the BigQuery partitioning metadata."""
        clustering = None
        if self.bigquery and self.bigquery.clustering:
            clustering = self.bigquery.clustering

        self.bigquery = BigQueryMetadata(
            time_partitioning=PartitionMetadata(
                field=field,
                type=PartitionType(partition_type),
                require_partition_filter=required,
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
