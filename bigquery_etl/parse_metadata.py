"""
Parsing of metadata yaml files.
"""

import re
import yaml


class Metadata:
    def __init__(self, friendly_name=None, description=None, labels={}):
        self.friendly_name = friendly_name
        self.description = description
        self.labels = labels


    @staticmethod
    def is_valid_label(label):
        """
        Check if a label has the right format:
        Only hyphens (-), underscores (_), lowercase characters, and numbers are allowed.
        International characters are allowed.
        """
        return re.match(r"[\w\d_-]+", label)


    @classmethod
    def from_file(cls, metadata_file):
        friendly_name = None
        description = None
        labels = {}

        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)

                if "friendly_name" in metadata:
                    friendly_name = metadata["friendly_name"]

                if "description" in metadata:
                    description = metadata["description"]

                if "labels" in metadata:
                    labels = {}

                    for key, label in metadata["labels"].items():
                        if isinstance(label, bool) and Metadata.is_valid_label(
                            str(key)
                        ):
                            # key-value pair with boolean value should get published as tag
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


    def is_public_bigquery(self):
        """
        Return true if the public_bigquery flag is set.
        """

        return "public_bigquery" in self.labels
