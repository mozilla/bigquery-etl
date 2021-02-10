"""Query schema."""

import attr
import json
import yaml

from google.cloud import bigquery
from pathlib import Path
from typing import Dict, Any


@attr.s(auto_attribs=True)
class Schema:
    """Query schema representation and helpers."""

    schema: Dict[str, Any]

    @classmethod
    def from_query_file(cls, query_file: Path):
        if not query_file.is_file() or query_file.suffix != ".sql":
            raise Exception(f"{query_file} is not a valid SQL file.")

        with open(query_file) as query:
            sql = query.read()
            dataset = query_file.parent.parent.name
            project = query_file.parent.parent.parent.name
            schema = cls._schema_from_dry_run(sql, dataset, project)
            return cls(schema)

    @classmethod
    def from_schema_file(cls, schema_file: Path):
        """Create schema from a yaml schema file."""
        if not schema_file.is_file() or schema_file.suffix != ".yml":
            raise Exception(f"{schema_file} is not a valid YAML schema file.")

        with open(schema_file) as file:
            schema = yaml.load(file, Loader=yaml.FullLoader)
            return cls(schema)

    @classmethod
    def from_json(cls, json_schema):
        """Create schema from JSON object."""
        return cls(json_schema)

    def merge(self, other: "Schema"):
        """Merge another schema into the schema."""
        self._traverse(self.schema["fields"], other["fields"], update=True)

    def equal(self, other: "Schema") -> bool:
        """Compare to another schema"""
        try:
            self._traverse(self.schema["fields"], other["fields"], update=False)
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
                else:
                    raise Exception(f"{prefix}.{field_path} is missing in schema")

    def to_yaml_file(self, yaml_path: Path):
        """Write schema to the YAML file path."""
        with open(yaml_path, "w") as out:
            yaml.dump(self.schema, out, default_flow_style=False)

    def to_json(self):
        """Return the schema data as JSON."""
        return json.dumps(self.schema)

    # todo: change to use dryRun function once schema information gets returned
    @staticmethod
    def _schema_from_dry_run(sql, project, dataset) -> Dict[str, Any]:
        """Perform a dry run to get schema information."""
        config = bigquery.QueryJobConfig(
            dry_run=True,
            default_dataset=f"{project}.{dataset}",
            query_parameters=[
                bigquery.ScalarQueryParameter("submission_date", "DATE", "2000-01-01"),
            ],
        )
        client = bigquery.Client()
        schema = client.query(sql, config)._job_statistics()["schema"]
        return schema
