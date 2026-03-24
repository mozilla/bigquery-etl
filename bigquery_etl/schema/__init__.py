"""Query schema."""

import json
import os
from functools import cache, lru_cache, wraps
from pathlib import Path
from typing import Any, Callable, Concatenate, Dict, Iterable, List, Optional

import attr
import ujson
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from jmespath import search as jmespath_search

from .. import dryrun
from ..config import ConfigLoader
from ..util.common import resolve_project_file_path
from .stable_table_schema import get_stable_table_schemas

SCHEMA_FILE = "schema.yaml"
DEFAULT_SQL_DIR = Path(ConfigLoader.get("default", "sql_dir"))


@attr.s(auto_attribs=True)
class Schema:
    """Query schema representation and helpers."""

    schema: Dict[str, Any]
    _type_mapping: Dict[str, str] = {
        "INT64": "INTEGER",
        "BOOL": "BOOLEAN",
        "FLOAT64": "FLOAT",
    }

    @classmethod
    def from_query_file(cls, query_file: Path, *args, **kwargs):
        """Create schema from a query file."""
        if not query_file.is_file() or query_file.suffix != ".sql":
            raise Exception(f"{query_file} is not a valid SQL file.")

        schema = dryrun.DryRun(str(query_file), *args, **kwargs).get_schema()
        return cls(schema)

    @classmethod
    def from_yaml(cls, schema_yaml: str, sql_dir: Optional[Path] = None):
        """Create schema from a YAML string."""
        yaml_loader = SchemaLoader
        if sql_dir and sql_dir != DEFAULT_SQL_DIR:
            custom_sql_dir = sql_dir  # To avoid a NameError in CustomSchemaLoader.

            class CustomSchemaLoader(SchemaLoader):
                sql_dir = custom_sql_dir

            yaml_loader = CustomSchemaLoader

        schema = yaml.load(schema_yaml, Loader=yaml_loader)
        return cls(schema)

    @classmethod
    def from_schema_file(cls, schema_file: Path, sql_dir: Optional[Path] = None):
        """Create schema from a `schema.yaml` file."""
        if not schema_file.is_file() or schema_file.suffix != ".yaml":
            raise Exception(f"{schema_file} is not a valid YAML schema file.")

        return cls.from_yaml(schema_file.read_text(), sql_dir=sql_dir)

    @classmethod
    def empty(cls):
        """Create an empty schema."""
        return cls({"fields": []})

    @classmethod
    def from_json(cls, json_schema):
        """Create schema from JSON object."""
        return cls(json_schema)

    @classmethod
    def for_table(
        cls,
        project,
        dataset,
        table,
        partitioned_by=None,
        filename="query.sql",
        *args,
        **kwargs,
    ):
        """Get the schema for a BigQuery table."""
        query = f"SELECT * FROM `{project}.{dataset}.{table}`"

        if partitioned_by:
            query += f" WHERE DATE(`{partitioned_by}`) = DATE('2020-01-01')"

        try:
            sql_dir = ConfigLoader.get("default", "sql_dir")
            return cls(
                dryrun.DryRun(
                    os.path.join(sql_dir, project, dataset, table, filename),
                    query,
                    project=project,
                    dataset=dataset,
                    table=table,
                    *args,
                    **kwargs,
                ).get_table_schema()
            )
        except Exception as e:
            print(f"Cannot get schema for {project}.{dataset}.{table}: {e}")
            return cls({"fields": []})

    def get_field(self, field_path: str) -> dict:
        """Return the specified schema field."""
        fields = self.schema["fields"]

        if "." in field_path:
            parent_field_path, field_name = field_path.rsplit(".", maxsplit=1)
            parent_field_path_parts = parent_field_path.split(".")
            for i, parent_field_path_part in enumerate(parent_field_path_parts):
                for field in fields:
                    if field["name"] == parent_field_path_part:
                        fields = field["fields"]
                        break
                else:
                    raise KeyError(
                        f"Field not found: {'.'.join(parent_field_path_parts[0:i+1])}"
                    )
        else:
            field_name = field_path

        for field in fields:
            if field["name"] == field_name:
                return field
        else:
            raise KeyError(f"Field not found: {field_path}")

    def deploy(self, destination_table: str) -> bigquery.Table:
        """Deploy the schema to BigQuery named after destination_table."""
        client = bigquery.Client()
        bigquery_schema = self.to_bigquery_schema()

        try:
            # destination table already exists, update schema
            table = client.get_table(destination_table)
            table.schema = bigquery_schema
            return client.update_table(table, ["schema"])
        except NotFound:
            table = bigquery.Table(destination_table, schema=bigquery_schema)
            return client.create_table(table)

    def merge(
        self,
        other: "Schema",
        exclude: Optional[List[str]] = None,
        add_missing_fields=True,
        attributes: Optional[List[str]] = None,
        ignore_incompatible_fields: bool = False,
        ignore_missing_fields: bool = False,
    ):
        """Merge another schema into the schema."""
        if "fields" in other.schema and "fields" in self.schema:
            self._traverse(
                "root",
                self.schema["fields"],
                other.schema["fields"],
                update=True,
                exclude=exclude,
                add_missing_fields=add_missing_fields,
                attributes=attributes,
                ignore_incompatible_fields=ignore_incompatible_fields,
                ignore_missing_fields=ignore_missing_fields,
            )

    def equal(self, other: "Schema") -> bool:
        """Compare to another schema."""
        try:
            self._traverse(
                "root", self.schema["fields"], other.schema["fields"], update=False
            )
            self._traverse(
                "root", other.schema["fields"], self.schema["fields"], update=False
            )
        except Exception as e:
            print(e)
            return False

        return True

    def compatible(self, other: "Schema") -> bool:
        """
        Check if schema is compatible with another schema.

        If there is a field missing in the schema that is part of the "other" schema,
        the schemas are still compatible. However, if there are fields missing in the
        "other" schema they are not compatible since, e.g. inserting data into the "other"
        schema that follows this schema would fail.
        """
        try:
            self._traverse(
                "root",
                self.schema["fields"],
                other.schema["fields"],
                update=False,
                ignore_missing_fields=True,
            )
            self._traverse(
                "root",
                other.schema["fields"],
                self.schema["fields"],
                update=False,
                ignore_missing_fields=False,
            )
        except Exception as e:
            print(e)
            return False

        return True

    @staticmethod
    def _node_with_mode(node):
        """Add default value for mode to node."""
        if "mode" in node:
            return node
        return {"mode": "NULLABLE", **node}

    def _traverse(
        self,
        prefix,
        columns,
        other_columns,
        update=False,
        add_missing_fields=True,
        ignore_missing_fields=False,
        exclude=None,
        attributes=None,
        ignore_incompatible_fields=False,
    ):
        """Traverses two schemas for validation and optionally updates the first schema."""
        nodes = {n["name"]: Schema._node_with_mode(n) for n in columns}
        other_nodes = {
            n["name"]: Schema._node_with_mode(n)
            for n in other_columns
            if exclude is None or n["name"] not in exclude
        }

        for node_name, node in other_nodes.items():
            field_path = node["name"] + (".[]" if node["mode"] == "REPEATED" else "")
            dtype = node["type"]

            if node_name in nodes:
                # node exists in schema, update attributes where necessary
                for node_attr_key, node_attr_value in node.items():
                    if attributes and node_attr_key not in attributes:
                        continue

                    if node_attr_key == "type":
                        # sometimes types have multiple names (e.g. INT64 and INTEGER)
                        # make it consistent here
                        node_attr_value = self._type_mapping.get(
                            node_attr_value, node_attr_value
                        )
                        nodes[node_name][node_attr_key] = self._type_mapping.get(
                            nodes[node_name][node_attr_key],
                            nodes[node_name][node_attr_key],
                        )

                    if node_attr_key not in nodes[node_name]:
                        if update:
                            # add field attributes if not exists in schema
                            nodes[node_name][node_attr_key] = node_attr_value
                            # Netlify has a problem starting 2022-03-07 where lots of
                            # logging slows down builds to the point where our builds hit
                            # the time limit and fail (bug 1761292), and this print
                            # statement accounts for 84% of our build logging.
                            # TODO: Uncomment this print when Netlify fixes the problem.
                            # print(
                            #    f"Attribute {node_attr_key} added to {prefix}.{field_path}"
                            # )
                        else:
                            if node_attr_key == "description":
                                print(
                                    "Warning: descriptions for "
                                    f"{prefix}.{field_path} differ"
                                )
                            else:
                                if not ignore_incompatible_fields:
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
                            if not ignore_incompatible_fields:
                                raise Exception(
                                    f"Cannot merge schemas. {node_attr_key} attributes "
                                    f"for {prefix}.{field_path} are incompatible"
                                )

                if dtype == "RECORD" and nodes[node_name]["type"] == "RECORD":
                    # keep traversing nested fields
                    self._traverse(
                        f"{prefix}.{field_path}",
                        nodes[node_name]["fields"],
                        node["fields"],
                        update=update,
                        add_missing_fields=add_missing_fields,
                        ignore_missing_fields=ignore_missing_fields,
                        attributes=attributes,
                        ignore_incompatible_fields=ignore_incompatible_fields,
                    )
            else:
                if update and add_missing_fields:
                    # node does not exist in schema, add to schema
                    if node["type"] == "RECORD":  # deep copy record fields
                        columns.append(ujson.loads(ujson.dumps(node)))
                    else:
                        columns.append(node.copy())
                    print(f"Field {node_name} added to {prefix}")
                else:
                    if not ignore_missing_fields:
                        raise Exception(
                            f"Field {prefix}.{field_path} is missing in schema"
                        )

    def to_yaml_file(self, yaml_path: Path):
        """Write schema to the YAML file path."""
        with open(yaml_path, "w") as out:
            yaml.dump(
                self.schema,
                out,
                Dumper=SchemaDumper,
                default_flow_style=False,
                sort_keys=False,
            )

    def to_yaml(self):
        """Return the schema data as YAML."""
        return yaml.dump(
            self.schema, Dumper=SchemaDumper, default_flow_style=False, sort_keys=False
        )

    def to_json_file(self, json_path: Path):
        """Write schema to the JSON file path."""
        with open(json_path, "w") as out:
            json.dump(self.schema["fields"], out, indent=2)

    def to_json(self):
        """Return the schema data as JSON."""
        return json.dumps(self.schema)

    def to_bigquery_schema(self) -> List[SchemaField]:
        """Get the BigQuery representation of the schema."""
        return [SchemaField.from_api_repr(field) for field in self.schema["fields"]]

    @classmethod
    def from_bigquery_schema(cls, fields: List[SchemaField]) -> "Schema":
        """Construct a Schema from the BigQuery representation."""
        return cls({"fields": [field.to_api_repr() for field in fields]})

    def generate_compatible_select_expression(
        self,
        target_schema: "Schema",
        fields_to_remove: Optional[Iterable[str]] = None,
        unnest_structs: bool = False,
        max_unnest_depth: int = 0,
        unnest_allowlist: Optional[Iterable[str]] = None,
        match_column_order: bool = True,
        retain_null_structs: bool = False,
    ) -> str:
        """Generate the select expression for the source schema based on the target schema.

        The output will include all fields of the target schema in the same order of the target.
        Any fields that are missing in the source schema are set to NULL.

        :param target_schema: The schema to coerce the current schema to.
        :param fields_to_remove: Given fields are removed from the output expression. Expressed as a
            list of strings with `.` separating each level of nesting, e.g. record_name.field.
        :param unnest_structs: If true, all record fields are expressed as structs with all nested
            fields explicitly listed. This allows the expression to be compatible even if the
            source schemas get new fields added. Otherwise, records are only unnested if they
            do not match the target schema.
        :param max_unnest_depth: Maximum level of struct nesting to explicitly unnest in
            the expression.
        :param unnest_allowlist: If set, only the given top-level structs are unnested.
        :param match_column_order: If true, structs will be unnested if the order of the nested
            fields does not match. This is needed if column order matters, e.g. UNION queries.
        :param retain_null_structs: If true, structs will be set to null if they are null in the
            source. Otherwise, structs will always be non-null with nested fields explicitly set.
        """

        def _type_info(node):
            """Determine the BigQuery type information from Schema object field."""
            dtype = node["type"]
            if dtype == "RECORD":
                dtype = (
                    "STRUCT<"
                    + ", ".join(
                        f"`{field['name']}` {_type_info(field)}"
                        for field in node["fields"]
                    )
                    + ">"
                )
            elif dtype == "FLOAT":
                dtype = "FLOAT64"
            if node.get("mode") == "REPEATED":
                return f"ARRAY<{dtype}>"
            return dtype

        def recurse_fields(
            _source_schema_nodes: List[Dict],
            _target_schema_nodes: List[Dict],
            path=None,
        ) -> str:
            if path is None:
                path = []

            select_expr = []
            source_schema_nodes = {n["name"]: n for n in _source_schema_nodes}
            target_schema_nodes = {n["name"]: n for n in _target_schema_nodes}

            # iterate through fields
            for node_name, node in target_schema_nodes.items():
                dtype = node["type"]
                node_path = path + [node_name]
                node_path_str = ".".join(node_path)

                if node_name in source_schema_nodes:  # field exists in app schema
                    # field matches, can query as-is
                    if (
                        dtype == source_schema_nodes[node["name"]]["type"]
                        and (
                            # fields must match for records, otherwise only type needs to match
                            # note that this will be false if field descriptions don't match
                            dtype != "RECORD"
                            or node["fields"]
                            == source_schema_nodes[node_name]["fields"]
                            or (
                                # check if top-level fields match in any order
                                not match_column_order
                                and len(node["fields"])
                                == len(source_schema_nodes[node_name]["fields"])
                                and sorted(
                                    source_schema_nodes[node_name]["fields"],
                                    key=lambda f: f["name"],
                                )
                                == sorted(node["fields"], key=lambda f: f["name"])
                            )
                        )
                        and (
                            # don't need to unnest scalar
                            dtype != "RECORD"
                            or not unnest_structs
                            # reached max record depth to unnest
                            or len(node_path) > max_unnest_depth > 0
                            # field not in unnest allowlist
                            or (
                                unnest_allowlist is not None
                                and node_path[0] not in unnest_allowlist
                            )
                        )
                    ):
                        if (
                            fields_to_remove is None
                            or node_path_str not in fields_to_remove
                        ):
                            select_expr.append(node_path_str)
                    elif (
                        dtype == "RECORD"
                    ):  # for nested fields, recursively generate select expression
                        if (
                            node.get("mode", None) == "REPEATED"
                        ):  # unnest repeated record
                            select_expr.append(f"""
                                    ARRAY(
                                        SELECT
                                            STRUCT(
                                                {recurse_fields(
                                                    source_schema_nodes[node_name]['fields'],
                                                    node['fields'],
                                                    [node_name],
                                                )}
                                            )
                                        FROM UNNEST({node_path_str}) AS `{node_name}`
                                    ) AS `{node_name}`
                                """)
                        else:  # select struct fields
                            struct_expr = f"""
                            STRUCT(
                                {recurse_fields(
                                    source_schema_nodes[node_name]['fields'],
                                    node['fields'],
                                    node_path,
                                )}
                            )
                            """
                            if retain_null_structs:
                                select_expr.append(f"""
                                    IF(
                                        {node_path_str} IS NULL,
                                        NULL,
                                        {struct_expr}
                                    ) AS `{node_name}`
                                """)
                            else:
                                select_expr.append(f"{struct_expr} AS `{node_name}`")
                    else:  # scalar value doesn't match, e.g. different types
                        select_expr.append(
                            f"CAST(NULL AS {_type_info(node)}) AS `{node_name}`"
                        )
                else:  # field not found in source schema
                    select_expr.append(
                        f"CAST(NULL AS {_type_info(node)}) AS `{node_name}`"
                    )

            return ", ".join(select_expr)

        return recurse_fields(
            self.schema["fields"],
            target_schema.schema["fields"],
        )

    def generate_select_expression(
        self,
        remove_fields: Optional[Iterable[str]] = None,
        unnest_structs: bool = False,
        max_unnest_depth: int = 0,
        unnest_allowlist: Optional[Iterable[str]] = None,
    ) -> str:
        """Generate the select expression for the schema which includes each field."""
        return self.generate_compatible_select_expression(
            self,
            remove_fields,
            unnest_structs,
            max_unnest_depth,
            unnest_allowlist,
        )


@cache
def generate_compatible_select_expression(
    client: bigquery.Client, v1_table_id: str, v2_table_id: str
):
    """Generate a select expression that conforms the v1 table schema to the v2 table schema.

    The expression will be compatible with the v2 table. Fields that are in v1 but not v2 will
    not be included. Fields that are in v2 but not v1 will be set to null.
    """
    v1_table = client.get_table(v1_table_id)
    v2_table = client.get_table(v2_table_id)

    v1_schema = Schema.from_bigquery_schema(v1_table.schema)
    v2_schema = Schema.from_bigquery_schema(v2_table.schema)

    return v1_schema.generate_compatible_select_expression(
        v2_schema, match_column_order=False, retain_null_structs=True
    )


class SchemaLoader(yaml.FullLoader):
    """Loader for schema YAML files."""

    sql_dir = DEFAULT_SQL_DIR

    @lru_cache
    def load_yaml_from_project_file(self, project_file_path: str | Path) -> Any:
        """Load the YAML from the specified project file."""
        project_file_path = Path(str(project_file_path).removeprefix("/"))
        if self.sql_dir != DEFAULT_SQL_DIR and project_file_path.is_relative_to(
            DEFAULT_SQL_DIR
        ):
            resolved_project_file_path = Path(
                self.sql_dir, project_file_path.relative_to(DEFAULT_SQL_DIR)
            )
        else:
            resolved_project_file_path = resolve_project_file_path(project_file_path)
        with resolved_project_file_path.open() as file_stream:
            return yaml.load(file_stream, Loader=self.__class__)

    def get_schema_from_project_file(self, project_file_path: str | Path) -> Schema:
        """Get the schema from the specified project file."""
        return Schema(self.load_yaml_from_project_file(project_file_path))

    def get_schema_for_table(self, table: str) -> Schema:
        """Get the schema for the specified table."""
        if "_stable." in table:
            for stable_table_schema in get_stable_table_schemas():
                if table.endswith("." + stable_table_schema.stable_table):
                    return Schema({"fields": stable_table_schema.schema})
            else:
                raise Exception(f"Stable table schema not found: {table}")
        return self.get_schema_from_project_file(
            Path(DEFAULT_SQL_DIR, *table.split("."), SCHEMA_FILE)
        )


def yaml_tag_kwargs_loader(
    yaml_tag_constructor: Callable[Concatenate[SchemaLoader, ...], Any],
) -> Callable[[SchemaLoader, yaml.Node], Any]:
    """Decorate a YAML tag constructor, passing it the keyword arguments loaded from the YAML dictionary node."""

    @wraps(yaml_tag_constructor)
    def yaml_tag_constructor_wrapper(loader: SchemaLoader, node: yaml.Node) -> Any:
        kwargs: dict[str, Any] = loader.construct_mapping(node, deep=True)  # type: ignore
        return yaml_tag_constructor(loader, **kwargs)

    return yaml_tag_constructor_wrapper


@yaml_tag_kwargs_loader
def yaml_include_constructor(
    loader: SchemaLoader, file: str, jmespath: Optional[str] = None
) -> Any:
    """Load a YAML `!include` tag."""
    data = loader.load_yaml_from_project_file(file)
    if jmespath:
        return jmespath_search(jmespath, data)
    return data


SchemaLoader.add_constructor("!include", yaml_include_constructor)


@yaml_tag_kwargs_loader
def yaml_include_field_constructor(
    loader: SchemaLoader,
    field: str,
    file: Optional[str] = None,
    table: Optional[str] = None,
    new_name: Optional[str] = None,
    new_type: Optional[str] = None,
    new_mode: Optional[str] = None,
    new_description: Optional[str] = None,
    append_description: Optional[str] = None,
    prepend_description: Optional[str] = None,
    new_fields: Optional[list[dict]] = None,
) -> dict:
    """Load a YAML `!include-field` tag."""
    if file:
        schema = loader.get_schema_from_project_file(file)
    elif table:
        schema = loader.get_schema_for_table(table)
    else:
        raise Exception("!include-field tags must specify either `file` or `table`.")

    schema_field = schema.get_field(field)
    if new_name:
        schema_field["name"] = new_name
    if new_type:
        schema_field["type"] = new_type
    if new_mode:
        schema_field["mode"] = new_mode
    if new_description:
        schema_field["description"] = new_description
    if append_description or prepend_description:
        description: str = schema_field.get("description", "")
        if append_description:
            description = (description.rstrip() + "\n" + append_description).lstrip()
        if prepend_description:
            description = (prepend_description + "\n" + description.lstrip()).rstrip()
        schema_field["description"] = description
    if new_fields:
        schema_field["fields"] = new_fields
    return schema_field


SchemaLoader.add_constructor("!include-field", yaml_include_field_constructor)


@yaml_tag_kwargs_loader
def yaml_include_fields_constructor(
    loader: SchemaLoader,
    file: Optional[str] = None,
    table: Optional[str] = None,
    parent_field: Optional[str] = None,
    field_names: Optional[list[str]] = None,
    exclude_field_names: Optional[list[str]] = None,
    field_replacements: Optional[list[dict]] = None,
) -> list[dict]:
    """Load a YAML `!include-fields` tag."""
    if file:
        schema = loader.get_schema_from_project_file(file)
    elif table:
        schema = loader.get_schema_for_table(table)
    else:
        raise Exception("!include-fields tags must specify either `file` or `table`.")

    fields = schema.schema["fields"]
    if parent_field:
        fields = schema.get_field(parent_field)["fields"]
    if field_names:
        fields_dict = {field["name"]: field for field in fields}
        # Return the fields in the same order as the specified field names.
        fields = [fields_dict[field_name] for field_name in field_names]
    if exclude_field_names:
        fields = [field for field in fields if field["name"] not in exclude_field_names]
    if field_replacements:
        field_replacements_dict = {field["name"]: field for field in field_replacements}
        fields = [field_replacements_dict.get(field["name"], field) for field in fields]
    return fields


SchemaLoader.add_constructor("!include-fields", yaml_include_fields_constructor)


@yaml_tag_kwargs_loader
def yaml_include_field_description_constructor(
    loader: SchemaLoader,
    field: str,
    file: Optional[str] = None,
    table: Optional[str] = None,
    append: Optional[str] = None,
    prepend: Optional[str] = None,
) -> str:
    """Load a YAML `!include-field-description` tag."""
    if file:
        schema = loader.get_schema_from_project_file(file)
    elif table:
        schema = loader.get_schema_for_table(table)
    else:
        raise Exception(
            "!include-field-description tags must specify either `file` or `table`."
        )

    description: str = schema.get_field(field).get("description", "")
    if append:
        description = (description.rstrip() + "\n" + append).lstrip()
    if prepend:
        description = (prepend + "\n" + description.lstrip()).rstrip()
    return description


SchemaLoader.add_constructor(
    "!include-field-description",
    yaml_include_field_description_constructor,
)


def yaml_flatten_lists_constructor(loader: SchemaLoader, node: yaml.Node) -> list:
    """Load a YAML `!flatten-lists` tag."""
    flattened_list = []
    for item in loader.construct_sequence(node, deep=True):  # type: ignore
        if isinstance(item, list):
            flattened_list.extend(item)
        else:
            flattened_list.append(item)
    return flattened_list


SchemaLoader.add_constructor("!flatten-lists", yaml_flatten_lists_constructor)


class SchemaDumper(yaml.Dumper):
    """Dumper for schema YAML files."""

    def ignore_aliases(self, data):
        """Never use aliases in the output."""
        return True
