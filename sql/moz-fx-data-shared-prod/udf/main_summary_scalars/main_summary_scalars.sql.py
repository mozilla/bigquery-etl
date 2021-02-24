#!/usr/bin/env python3
"""Generate udf/main_summary_scalars.sql."""

import itertools
import json
import os.path
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.resolve()))
from bigquery_etl.util.common import snake_case  # noqa E402

SCALAR_TYPES = {"uint": "INT64", "string": "STRING", "boolean": "BOOL"}


def collect_probes(probes, schema_fields):
    """Collect scalars from probes and schema fields."""
    scalars = {"main": [], "content": [], "gpu": []}
    keyed_scalars = {"main": [], "content": [], "gpu": []}
    for probe in probes:
        history = list(itertools.chain.from_iterable(probe["history"].values()))
        record_in_processes = set().union(
            *[h["details"]["record_in_processes"] for h in history]
        )
        if probe["type"] == "scalar":
            collection = (
                keyed_scalars
                if any(h["details"]["keyed"] for h in history)
                else scalars
            )
        else:
            continue
        if record_in_processes == set(["all"]):
            record_in_processes = ["main", "content", "gpu"]
        if "all_children" in record_in_processes or "all_childs" in record_in_processes:
            record_in_processes = ["content", "gpu"]
        for p in record_in_processes:
            collection[p].append(
                (
                    snake_case(probe["name"]).replace(".", "_"),
                    SCALAR_TYPES.get(
                        history[0]["details"]["kind"], history[0]["details"]["kind"]
                    ),
                )
            )
    return scalars, keyed_scalars


def search(target, path):
    """List the field names available in target at path."""
    for key in path:
        for item in target:
            if item["name"] == key:
                target = item["fields"]
                break
    return set(x["name"] for x in target)


def collect_fields(main_schema):
    """Collect scalar fields from main ping schema."""
    scalars = {
        "main": search(main_schema, ["payload", "processes", "parent", "scalars"]),
        "content": search(main_schema, ["payload", "processes", "content", "scalars"]),
    }
    keyed_scalars = {
        "main": search(
            main_schema, ["payload", "processes", "parent", "keyed_scalars"]
        ),
        "content": search(
            main_schema, ["payload", "processes", "content", "keyed_scalars"]
        ),
    }
    return {"scalars": scalars, "keyed_scalars": keyed_scalars}


def make_field(source, target, s, schema_fields, keyed=False):
    """Build a scalar definition."""
    (name, sql_type) = s
    if name in schema_fields:
        source = "processes.%s.%s" % (source, name)
    else:
        source = "JSON_EXTRACT(additional_properties, '$.payload.processes.%s.%s')" % (
            source,
            name,
        )
        if keyed:
            kind = {"BOOL": "bool", "INT64": "string_int"}[sql_type]
            source = "udf.json_extract_%s_map(%s)" % (kind, source)
        else:
            source = "CAST(%s AS %s)" % (source, sql_type)
    return "%s AS %s_%s" % (source, target, name)


def main(root):
    """Generate udf/main_summary_scalars.sql."""
    main_schema_json = open("main.4.bq")
    main_schema = json.load(main_schema_json)
    schema_fields = collect_fields(main_schema)

    probes_json = open("all_probes.json")
    probes = json.load(probes_json).values()
    scalars, keyed_scalars = collect_probes(probes, schema_fields)
    scalar_fields = ",\n    ".join(
        [
            make_field(
                "parent.scalars", "scalar_parent", s, schema_fields["scalars"]["main"]
            )
            for s in scalars["main"]
        ]
        + [
            make_field(
                "content.scalars",
                "scalar_content",
                s,
                schema_fields["scalars"]["content"],
            )
            for s in scalars["content"]
        ]
        + [
            make_field(
                "parent.keyed_scalars",
                "scalar_parent",
                s,
                schema_fields["keyed_scalars"]["main"],
                True,
            )
            for s in keyed_scalars["main"]
        ]
        + [
            make_field(
                "content.keyed_scalars",
                "scalar_content",
                s,
                schema_fields["keyed_scalars"]["content"],
                True,
            )
            for s in keyed_scalars["content"]
        ]
    )

    scalars_sql = """
CREATE OR REPLACE FUNCTION udf.main_summary_scalars(processes ANY TYPE, additional_properties STRING) AS (
  STRUCT(
    %s
  )
);
""" % (  # noqa E501
        scalar_fields,
    )

    open(os.path.join(root, "main_summary_scalars.sql"), "w").write(scalars_sql)


if __name__ == "__main__":
    main(os.path.dirname(__file__))
