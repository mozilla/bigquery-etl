import click
import os


def is_valid_dir(ctx, param, value):
    if not os.path.isdir(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid directory path to {value}")
    return value


def is_valid_file(ctx, param, value):
    if not os.path.isfile(value) or not os.path.exists(value):
        raise click.BadParameter(f"Invalid file path to {value}")
    return value
