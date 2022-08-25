"""Generate docs for bqetl commands."""

import os
from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.dag import dag
from bigquery_etl.cli.dryrun import dryrun
from bigquery_etl.cli.format import format
from bigquery_etl.cli.query import query
from bigquery_etl.cli.routine import mozfun, routine
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
}
FILE_PATH = Path(os.path.dirname(__file__))
BASE_DIR = FILE_PATH.parent.parent
TEMPLATE = FILE_PATH / "templates" / "commands.md"


def extract_description_from_help_text(help_text):
    """Return the description from the command help text."""
    return help_text.split("\n\n")[0].strip()


def extract_examples_from_help_text(help_text):
    """Return the examples from the command help text."""
    examples = None
    help_text = help_text.split("\n\n")

    if len(help_text) > 1:
        examples = "\n\n".join(help_text[1:])
        examples = examples.replace("Examples:\n\n", "")
        # Examples have \b markers to preserve formatting for the following block
        # when rendered using Click:
        # https://click.palletsprojects.com/en/7.x/documentation/#preventing-rewrapping
        # Removing it, otherwise it shows up as newline in rendered Markdown
        examples = examples.replace("\b", "")
        examples = examples.strip()

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


def parse_commands(cmd, path):
    """Parse click commands and store in dict."""
    commands = []
    # full command path; reflect click group nesting
    path = path + " " + cmd.name
    if isinstance(cmd, click.Group):
        commands = [parse_commands(c, path) for _, c in cmd.commands.items()]

    return {
        "name": cmd.name,
        "commands": commands,
        "examples": extract_examples_from_help_text(cmd.help),
        "description": extract_description_from_help_text(cmd.help),
        "options": extract_options_from_command(cmd),
        "arguments": extract_arguments_from_command(cmd),
        "path": path,
    }


def generate_bqetl_docs(out_file):
    """Generate documentation for bqetl CLI commands."""
    print("Generate bqetl command docs.")
    command_groups = [parse_commands(group, "") for _, group in COMMANDS.items()]

    # render md docs
    file_loader = FileSystemLoader(TEMPLATE.parent)
    env = Environment(loader=file_loader)
    template = env.get_template(TEMPLATE.name)
    output = template.render(command_groups=command_groups)

    # append to bqetl docs page
    with open(out_file, "a") as out:
        out.write(output)
