"""Parsing of metadata yaml files."""

import re
import yaml
import os


METADATA_FILE = "metadata.yaml"


class Metadata:
    """Representation of metadata file content."""

    def __init__(self, friendly_name=None, description=None, labels={}):
        """Create a new Metadata instance."""
        self.friendly_name = friendly_name
        self.description = description
        self.labels = labels

    @staticmethod
    def is_valid_label(label):
        """
        Check if a label has the right format.

        Only hyphens (-), underscores (_), lowercase characters, and
        numbers are allowed. International characters are allowed.

        Keys have a minimum length of 1 character and a maximum length of
        63 characters, and cannot be empty. Values can be empty, and have
        a maximum length of 63 characters.
        """
        return re.match(r"[\w\d_-]+", label) and len(label) <= 63

    @classmethod
    def from_file(cls, metadata_file):
        """Parse metadata from the provided file and create a new Metadata instance."""
        friendly_name = None
        description = None
        labels = {}

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
                                are allowed. International characters are allowed.
                                """.format(
                                    key, label
                                )
                            )

                return cls(friendly_name, description, labels)
            except yaml.YAMLError as e:
                raise e

    @classmethod
    def of_sql_file(cls, sql_file):
        """Return the metadata that is associated with the provided SQL file."""
        path, _ = os.path.split(sql_file)
        metadata_file = os.path.join(path, METADATA_FILE)
        cls = Metadata.from_file(metadata_file)
        return cls

    def is_public_bigquery(self):
        """Return true if the public_bigquery flag is set."""
        return "public_bigquery" in self.labels
