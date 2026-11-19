"""BigQuery ETL."""

import warnings

# Ignore annoying warnings from Google code (https://github.com/googleapis/google-cloud-python/issues/13974).
# TODO: Remove this filter when that Google Python SDK issue is resolved (or maybe once setuptools v81 is released).
warnings.filterwarnings("ignore", "pkg_resources is deprecated as an API")
