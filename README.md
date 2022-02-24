[![CircleCI](https://circleci.com/gh/mozilla/bigquery-etl.svg?style=shield&circle-token=742fb1108f7e6e5a28c11d43b21f62605037f5a4)](https://circleci.com/gh/mozilla/bigquery-etl)

# BigQuery ETL

This repository contains Mozilla Data Team's:

- Derived ETL jobs that do not require a custom container
- User-defined functions (UDFs)
- Airflow DAGs for scheduled bigquery-etl queries
- Tools for query & UDF deployment, management and scheduling

For more information, see [https://mozilla.github.io/bigquery-etl/](https://mozilla.github.io/bigquery-etl/)

## Quick Start

> Apple Silicon (M1) user suggestion
>
> Enable [Rosetta mode](https://support.apple.com/en-ca/HT211861) for your terminal _**BEFORE**_ installing below tools using your terminal. It'll save you a lot of headaches. For tips on maintaining parallel stacks of python and homebrew running with and without Rosetta, see blog posts from [Thinknum](https://medium.com/thinknum/how-to-install-python-under-rosetta-2-f98c0865e012) and [Sixty North](http://sixty-north.com/blog/pyenv-apple-silicon.html).

### Pre-requisites
- **Homebrew** (not required, but useful for Mac) - Follow the instructions [here](https://brew.sh/) to install homebrew on your Mac.
- **Python 3.8+** - (see [this guide](https://docs.python-guide.org/starting/install3/osx/) for instructions if you're on a mac and haven't installed anything other than the default system Python).
- **Java JDK 11+** - (required for some functionality, e.g. [AdoptOpenJDK](https://adoptium.net/?variant=openjdk11)) with `$JAVA_HOME` set.
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

4. Download java dependencies
```bash
mvn dependency:copy-dependencies
# specify `<(echo mozilla-bigquery-etl)` to retain bqetl from `./bqetl bootstrap`
venv/bin/pip-sync --pip-args=--no-deps requirements.txt java-requirements.txt <(echo mozilla-bigquery-etl)
```

Finally, if you are using Visual Studio Code, you may also wish to use our recommended defaults:
```bash
cp .vscode/settings.json.default .vscode/settings.json
```

And you should now be set up to start working in the repo! The easiest way to do this is for many tasks is to use [`bqetl`](https://mozilla.github.io/bigquery-etl/bqetl/). You may also want to read up on [common workflows](https://mozilla.github.io/bigquery-etl/cookbooks/common_workflows/).
