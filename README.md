[![CircleCI](https://dl.circleci.com/status-badge/img/gh/mozilla/bigquery-etl/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/mozilla/bigquery-etl/tree/main)
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
- **Java JDK 8+** - (required for some functionality, e.g. [AdoptOpenJDK](https://adoptium.net/)) with `$JAVA_HOME` set.
- **Maven** - (needed for downloading jar dependencies). Available via your package manager in most Linux distributions and from [homebrew](https://brew.sh/) on mac, or you can install yourself by [downloading a binary](https://maven.apache.org/download.cgi) and following maven's [install instructions](https://maven.apache.org/install.html).

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

4. Build and install java dependencies
```bash
mvn package
# specify `<(echo mozilla-bigquery-etl)` to retain bqetl from `./bqetl bootstrap`
venv/bin/pip-sync --pip-args=--no-deps requirements.txt <(echo mozilla-bigquery-etl)
```

Finally, if you are using Visual Studio Code, you may also wish to use our recommended defaults:
```bash
cp .vscode/settings.json.default .vscode/settings.json
cp .vscode/launch.json.default .vscode/launch.json
```

And you should now be set up to start working in the repo! The easiest way to do this is for many tasks is to use [`bqetl`](https://mozilla.github.io/bigquery-etl/bqetl/). You may also want to read up on [common workflows](https://mozilla.github.io/bigquery-etl/cookbooks/common_workflows/).
