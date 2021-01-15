import logging

from .commands import cli

logging.basicConfig(level=logging.DEBUG)
cli()
