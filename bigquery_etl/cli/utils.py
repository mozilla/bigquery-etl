"""Utility functions used by the CLI."""

import os
from pathlib import Path

import click
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


def is_authenticated(project_id="moz-fx-data-shared-prod"):
    """Check if the user is authenticated to GCP and can access the project."""
    client = bigquery.Client()
    return client.project == project_id


def is_valid_project(ctx, param, value):
    """Check if the provided project_id corresponds to an existing project."""
    if value is None or value in [Path(p).name for p in project_dirs()]:
        return value
    raise click.BadParameter(f"Invalid project {value}")
