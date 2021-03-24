"""Generate docs for bqetl commands."""

import click
from jinja2 import Environment, FileSystemLoader
import os
from pathlib import Path

from bigquery_etl.cli.dag import dag
from bigquery_etl.cli.dryrun import dryrun
from bigquery_etl.cli.format import format
from bigquery_etl.cli.query import query
from bigquery_etl.cli.routine import mozfun, routine
from bigquery_etl.cli.view import view
from bigquery_etl.dependency import dependency


# commands to document
COMMANDS = {
    "query": query,
    "dag": dag,
    "dependency": dependency,
    "dryrun": dryrun,
    "format": format,
    "routine": routine,
    "mozfun": mozfun,
    "view": view,
}
FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = FILE_PATH.parent.parent
TEMPLATE = FILE_PATH / "templates" / "commands.md"


def extract_description_from_help_text(help_text):
    """Return the description from the command help text."""
    return help_text.split("\n\n")[0]


def extract_examples_from_help_text(help_text):
    """Return the examples from the command help text."""
    examples = None
    help_text = help_text.split("\n\n")

    if len(help_text) > 1:
        examples = "\n\n".join(help_text[1:])
        examples = examples.replace("Examples:\n\n", "")

    return examples


def extract_options_from_command(cmd):
    """Extract options with descriptions from click command."""
    return [
        {"name": option.name, "description": option.help}
        for option in cmd.params
        if isinstance(option, click.Option)
    ]


def extract_arguments_from_command(cmd):
    """Extract options with descriptions from click command."""
    return [{"name": arg.name} for arg in cmd.params if isinstance(arg, click.Argument)]


def generate_bqetl_docs(out_file):
    """Generate documentation for bqetl CLI commands."""
    print("Generate bqetl command docs.")
    command_groups = []

    for command_group_name, command_group in COMMANDS.items():
        commands = []
        try:
            for _, command in command_group.commands.items():
                commands.append(
                    {
                        "name": command.name,
                        "description": extract_description_from_help_text(command.help),
                        "examples": extract_examples_from_help_text(command.help),
                        "options": extract_options_from_command(command),
                        "arguments": extract_arguments_from_command(command),
                    }
                )

            command_groups.append(
                {
                    "name": command_group_name,
                    "commands": commands,
                    "description": command_group.help,
                }
            )
        except Exception:
            # command is not a group, but simply a click.Command
            command_groups.append(
                {
                    "name": command_group_name,
                    "commands": [],
                    "examples": extract_examples_from_help_text(command_group.help),
                    "description": extract_description_from_help_text(
                        command_group.help
                    ),
                    "options": extract_options_from_command(command_group),
                    "arguments": extract_arguments_from_command(command_group),
                }
            )

    # render md docs
    file_loader = FileSystemLoader(TEMPLATE.parent)
    env = Environment(loader=file_loader)
    template = env.get_template(TEMPLATE.name)
    output = template.render(command_groups=command_groups)

    # append to bqetl docs page
    with open(out_file, "a") as out:
        out.write(output)


if __name__ == "__main__":
    generate_bqetl_docs()
