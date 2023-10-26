[![CircleCI](https://dl.circleci.com/status-badge/img/gh/mozilla/bigquery-etl/tree/main.svg?style=svg&circle-token=1df4cefd991043d7d3f13243ea80f38e7aa18341)](https://dl.circleci.com/status-badge/redirect/gh/mozilla/bigquery-etl/tree/main)
# BigQuery ETL

This repository contains Mozilla Data Team's:

- Derived ETL jobs that do not require a custom container
- User-defined functions (UDFs)
- Airflow DAGs for scheduled bigquery-etl queries
- Tools for query & UDF deployment, management and scheduling

For more information, see [https://mozilla.github.io/bigquery-etl/](https://mozilla.github.io/bigquery-etl/)

## Quick Start

> :exclamation: Apple Silicon (M1) user requirement
>
> Enable [Rosetta mode](https://support.apple.com/en-ca/HT211861) for your terminal _**BEFORE**_ installing below tools using your terminal. See our [M1 Mac
> Setup Guide](M1_MAC_SETUP.md) for more information

### Pre-requisites
- **Pyenv** (optional) Recommended if you want to install different versions of python, see instructions [here](https://github.com/pyenv/pyenv#basic-github-checkout). After the installation of pyenv, make sure that your terminal app is [configured to run the shell as a login shell](https://github.com/pyenv/pyenv/wiki/MacOS-login-shell).
- **Homebrew** (not required, but useful for Mac) - Follow the instructions [here](https://brew.sh/) to install homebrew on your Mac.
- **Python 3.10+** - (see [this guide](https://docs.python-guide.org/starting/install3/osx/) for instructions if you're on a mac and haven't installed anything other than the default system Python).

### GCP CLI tools

- **For Mozilla Employees or Contributors (not in Data Engineering)** - Set up GCP command line tools, [as described on docs.telemetry.mozilla.org](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#using-the-bq-command-line-tool). Note that some functionality (e.g. writing UDFs or backfilling queries) may not be allowed.
- **For Data Engineering** - In addition to setting up the command line tools, you will want to log in to `shared-prod` if making changes to production systems. Run `gcloud auth login --update-adc --project=moz-fx-data-shared-prod` (if you have not run it previously).

### Installing bqetl

1. Clone the repository
```bash
git clone git@github.com:mozilla/bigquery-etl.git
cd bigquery-etl
```

2. Install the `bqetl` command line tool
```bash
./bqetl bootstrap
```

3. Install standard pre-commit hooks
```bash
venv/bin/pre-commit install
```

Finally, if you are using Visual Studio Code, you may also wish to use our recommended defaults:
```bash
cp .vscode/settings.json.default .vscode/settings.json
cp .vscode/launch.json.default .vscode/launch.json
```

And you should now be set up to start working in the repo! The easiest way to do this is for many tasks is to use [`bqetl`](https://mozilla.github.io/bigquery-etl/bqetl/). You may also want to read up on [common workflows](https://mozilla.github.io/bigquery-etl/cookbooks/common_workflows/).

### License

The bigquery-etl is available as open source under the terms of the [MPL-2.0 license](https://github.com/mozilla/bigquery-etl/blob/main/LICENSE).
