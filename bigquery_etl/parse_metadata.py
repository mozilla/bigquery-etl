"""
Parsing of metadata yaml files.
"""

import re
import yaml


class Metadata:
    friendly_name = None
    description = None
    labels = {}

    @staticmethod
    def is_valid_label(label):
        """
        Check if a label has the right format:
        Only hyphens (-), underscores (_), lowercase characters, and numbers are allowed.
        International characters are allowed.
        """
        return re.match(r"[\w\d_-]+", label)

    @classmethod
    def from_file(self, metadata_file):
        with open(metadata_file, "r") as yaml_stream:
            try:
                metadata = yaml.safe_load(yaml_stream)

                if "friendly_name" in metadata:
                    self.friendly_name = metadata["friendly_name"]

                if "description" in metadata:
                    self.description = metadata["description"]

                if "labels" in metadata:
                    self.labels = {}

                    for key, label in metadata["labels"].items():
                        if isinstance(label, bool) and Metadata.is_valid_label(
                            str(key)
                        ):
                            # key-value pair with boolean value should get published as tag
                            if label:
                                self.labels[str(key)] = ""
                        elif Metadata.is_valid_label(
                            str(key)
                        ) and Metadata.is_valid_label(str(label)):
                            # all other pairs get published as key-value pair label
                            self.labels[str(key)] = str(label)
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

                return self
            except yaml.YAMLError as e:
                raise e
