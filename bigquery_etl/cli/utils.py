"""Utility functions used by the CLI."""

import os
import fnmatch
from pathlib import Path

import click
import re
from google.cloud import bigquery

from bigquery_etl.util.common import project_dirs


def is_valid_dir(ctx, param, value):
    """Check if the parameter provided via click is an existing directory."""
    if not os.path.isdir(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid directory path to {value}")
    return value


def is_valid_file(ctx, param, value):
    """Check if the parameter provided via click is an existing file."""
    if not os.path.isfile(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid file path to {value}")
    return value


def is_authenticated(project_id=None):
    """Check if the user is authenticated to GCP and can access the project."""
    client = bigquery.Client()

    if project_id:
        return client.project == project_id

    return True


def is_valid_project(ctx, param, value):
    """Check if the provided project_id corresponds to an existing project."""
    if value is None or value in [Path(p).name for p in project_dirs()]:
        return value
    raise click.BadParameter(f"Invalid project {value}")


def table_matches_patterns(pattern, invert, table):
    """Check if tables match pattern."""
    pattern = re.compile(fnmatch.translate(pattern))
    return (pattern.match(table) is not None) != invert
