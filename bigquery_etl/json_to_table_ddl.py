"""
Creates a Standard SQL DDL statement that recreates a table.

Takes in input json from a bigquery api or bq command line table
description.

See help for arguments. Currently does not implement field options or
table options besides require_partition_filter.

Example usage:
    bq show --format json {orignal_table} |
        json_to_table_ddl -d {new_project} -d {new_dataset}
"""


import argparse
import json
import sys

TEMPLATE = """
{table_create_mode_string} `{project}:{dataset}.{table_name}` (
    {columns_string}
){partition_string}{clustering_string}{table_options_string}
"""


create_modes = {
    "CREATE": "CREATE TABLE",
    "REPLACE": "CREATE OR REPLACE TABLE",
    "CREATE_IF_NOT_EXISTS": "CREATE TABLE IF NOT EXISTS",
}


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "-m",
    "--mode",
    help="Specifies creation mode for the table",
    choices=create_modes.keys(),
    default="create_if_not_exists",
    type=str.upper,
)
parser.add_argument("-p", "--project", help="Specifies project name for new table")
parser.add_argument("-d", "--dataset", help="Specifies dataset name for new table")
parser.add_argument("-t", "--table", help="Specifies new table name")
parser.add_argument(
    "input",
    nargs="?",
    help="JSON-formatted output from bq api or `bq show --format json <orignal table>`"
    " (can be provided via stdin)",
)


def _field_to_string(field, nest_level):
    tabs = "\t" * nest_level
    if field["type"] == "RECORD":
        subfields = ",\n".join(
            [_field_to_string(f, nest_level + 1) for f in field["fields"]]
        )
        definition = f"STRUCT<\n{subfields}>"
    else:
        definition = field["type"]

    mode = field.get("mode")

    if mode == "REPEATED":
        definition = f"ARRAY<{definition}>"
    elif mode == "REQUIRED":
        definition += " NOT NULL"

    return tabs + field["name"] + " " + definition


def main(argv):
    """Create a Standard SQL DDL statement that recreates a table."""
    args = parser.parse_args(argv[1:])
    if args.input:
        in_json = args.input
    else:
        with open("/dev/stdin") as i:
            in_json = i.read()

    orig = json.loads(in_json)
    table_create_mode_string = create_modes[args.mode]
    project = args.project if args.project else orig["tableReference"]["projectId"]
    dataset = args.dataset if args.dataset else orig["tableReference"]["datasetId"]
    table_name = args.table if args.table else orig["tableReference"]["tableId"]
    columns_string = ",\n".join(
        [_field_to_string(column, 1) for column in orig["schema"]["fields"]]
    )

    partition_string = ""
    clustering_string = ""
    table_options_string = ""
    table_options_list = []

    partition_obj = orig.get("timePartitioning")
    if partition_obj:
        try:
            field_type = [
                field
                for field in orig["schema"]["fields"]
                if field["name"] == partition_obj["field"]
            ][0]["type"]
        except IndexError:
            field_type = "TIMESTAMP"

        if field_type == "TIMESTAMP":
            partition_column = f"DATE({partition_obj['field']})"
        else:
            partition_column = partition_obj["field"]
        partition_string = f"\nPARTITION BY {partition_column}"
        if partition_obj["requirePartitionFilter"]:
            table_options_list.append("require_partition_filter=true")

    clustering_obj = orig.get("clustering")
    if clustering_obj:
        clustering_string = "\nCLUSTER BY " + ", ".join(clustering_obj["fields"])

    if len(table_options_list) > 0:
        table_options_string = (
            "\nOPTIONS (\n\t" + ",\n\t".join(table_options_list) + "\n)"
        )

    print(TEMPLATE.format(**locals()))


if __name__ == "__main__":
    main(sys.argv)
