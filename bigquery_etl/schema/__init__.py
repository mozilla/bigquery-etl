"""Query schema."""

import io
import attr
import json
import yaml

from google.cloud import bigquery
from pathlib import Path
from typing import Dict, Any

from bigquery_etl.dryrun import DryRun

SCHEMA_FILE = 'schema.yaml'


@attr.s(auto_attribs=True)
class Schema:
    """Query schema representation and helpers."""

    schema: Dict[str, Any]

    @classmethod
    def from_query_file(cls, query_file: Path):
        if not query_file.is_file() or query_file.suffix != ".sql":
            raise Exception(f"{query_file} is not a valid SQL file.")

        schema = DryRun(str(query_file)).get_schema()
        return cls(schema)

    @classmethod
    def from_schema_file(cls, schema_file: Path):
        """Create schema from a yaml schema file."""
        if not schema_file.is_file() or schema_file.suffix != ".yaml":
            raise Exception(f"{schema_file} is not a valid YAML schema file.")

        with open(schema_file) as file:
            schema = yaml.load(file, Loader=yaml.FullLoader)
            return cls(schema)

    @classmethod
    def from_json(cls, json_schema):
        """Create schema from JSON object."""
        return cls(json_schema)

    @classmethod
    def for_table(cls, project, dataset, table):
        """Get the schema for a BigQuery table."""
        client = bigquery.Client()
        table = client.get_table(f"{project}.{dataset}.{table}")
        table_schema = io.StringIO("")
        client.schema_to_json(table.schema, table_schema)
        return cls({"fields": json.loads(table_schema.getvalue())})

    def merge(self, other: "Schema"):
        """Merge another schema into the schema."""
        self._traverse(
            "root", self.schema["fields"], other.schema["fields"], update=True
        )

    def equal(self, other: "Schema") -> bool:
        """Compare to another schema"""
        try:
            self._traverse(
                "root", self.schema["fields"], other.schema["fields"], update=False
            )
        except Exception as e:
            print(e)
            return False

        return True

    def _traverse(self, prefix, columns, other_columns, update=False):
        """Traverses two schemas for validation and optionally updates the first schema."""
        nodes = {n["name"]: n for n in columns}
        other_nodes = {n["name"]: n for n in other_columns}

        for node_name, node in other_nodes.items():
            field_path = node["name"] + (".[]" if node["mode"] == "REPEATED" else "")
            dtype = node["type"]

            if node_name in nodes:
                # node exists in schema, update attributes where necessary
                for node_attr_key, node_attr_value in node.items():
                    if node_attr_key not in nodes[node_name]:
                        if update:
                            # add field attributes if not exists in schema
                            nodes[node_name][node_attr_key] = node_attr_value
                            print(
                                f"Attribute {node_attr_key} added to {prefix}.{field_path}"
                            )
                        else:
                            if node_attr_key == "description":
                                print(
                                    "Warning: descriptions for "
                                    f"{prefix}.{field_path} differ"
                                )
                            else:
                                raise Exception(
                                    f"{node_attr_key} missing in {prefix}.{field_path}"
                                )
                    elif nodes[node_name][node_attr_key] != node_attr_value:
                        # check field attribute diffs
                        if node_attr_key == "description":
                            # overwrite descripton for the "other" schema
                            print(
                                f"Warning: descriptions for {prefix}.{field_path} differ."
                            )
                        elif node_attr_key != "fields":
                            raise Exception(
                                "Cannot merge schemas. Field attributes "
                                f"for {prefix}.{field_path} are incompatible"
                            )

                if dtype == "RECORD":
                    # keep traversing nested fields
                    self._traverse(
                        f"{prefix}.{field_path}",
                        nodes[node_name]["fields"],
                        node["fields"],
                        update,
                    )
            else:
                if update:
                    # node does not exist in schema, add to schema
                    nodes[node_name] = node.copy()
                    print(f"Field {node_name} added to {prefix}")
                else:
                    raise Exception(f"{prefix}.{field_path} is missing in schema")

    def to_yaml_file(self, yaml_path: Path):
        """Write schema to the YAML file path."""
        with open(yaml_path, "w") as out:
            yaml.dump(self.schema, out, default_flow_style=False)

    def to_json(self):
        """Return the schema data as JSON."""
        return json.dumps(self.schema)
