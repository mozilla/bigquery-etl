"""Parsing of metadata yaml files."""

import re
import yaml
import os


METADATA_FILE = "metadata.yaml"


class Metadata:
    """Representation of metadata file content."""

    def __init__(
        self, friendly_name=None, description=None, owners=[], labels={}, scheduling={}
    ):
        """Create a new Metadata instance."""
        self.friendly_name = friendly_name
        self.description = description
        self.labels = labels
        self.scheduling = scheduling
        self.owners = owners

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

        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)

                friendly_name = metadata.get("friendly_name", None)
                description = metadata.get("description", None)

                if "labels" in metadata:
                    labels = {}

                    for key, label in metadata["labels"].items():
                        if isinstance(label, bool) and Metadata.is_valid_label(
                            str(key)
                        ):
                            # publish key-value pair with bool value as tag
                            if label:
                                labels[str(key)] = ""
                        elif Metadata.is_valid_label(
                            str(key)
                        ) and Metadata.is_valid_label(str(label)):
                            # all other pairs get published as key-value pair label
                            labels[str(key)] = str(label)
                        else:
                            print(
                                """
                                Invalid label format: {}: {}. Only hyphens (-),
                                underscores (_), lowercase characters, and numbers
                                are allowed. International characters are not allowed.
                                """.format(
                                    key, label
                                )
                            )

                if "scheduling" in metadata:
                    scheduling = metadata["scheduling"]

                if "owners" in metadata:
                    owners = metadata["owners"]

                return cls(friendly_name, description, owners, labels, scheduling)
            except yaml.YAMLError as e:
                raise e

    @classmethod
    def of_sql_file(cls, sql_file):
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

        file.write_text(yaml.dump(metadata_dict))

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

    def review_bug(self):
        """Return the bug ID of the data review bug in bugzilla."""
        return self.labels.get("review_bug", None)
