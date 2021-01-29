# Docker ETL

This repo is a collection of dockerized ETL jobs to increase discoverability 
of the source code of scheduled ETL.
There are also tools here that automate the common steps involved with creating and
scheduling an ETL job.
This includes defining a Docker image, setting up CI, and language boilerplate.
The primary use of this repo is to create Dockerized jobs that are pushed to GCR
so they can be scheduled via the Airflow GKE pod operator.

## Project Structure

### Jobs

Each job is located in its own directory in the `jobs/` directory, 
e.g. the contents of a job named `my-job` would go into `jobs/my-job`

All job directories should have a `Dockerfile`, a `ci_job.yaml`, 
a `ci_workflow.yaml`, and a `README.md` in the root directory.          
`ci_job.yaml` and `ci_workflow.yaml` contain the yaml structure that will be placed
in the `- jobs:` and `- workflows:` sections of the CircleCI `config.yml` respectively.

### Templates

Templates for job creation and the CI config file are located in `templates/`.

The CI config template is in `.circleci/config.template.yml`.
This is the file that should be modified instead of the `circleci/config.yml`.

Each job template is located in a directory in `templates/` that is the name of the template, 
e.g. a `python` template is in `templates/python/`.
Within the directory of a template is a directory named `job/` that contains
all the contents that will be copied when the template is used.
Other files in the directory of a particular template are used for
job creation, e.g. `ci_job.template.yaml`.

### Example Directory Structure:

```
+--docker-etl/
|  +--jobs/
|     +--example-python-1/
|        +--ci_job.yaml
|        +--ci_workflow.yaml
|        +--Dockerfile
|        +--README.md
|        +--script
|  +--templates/
|     +--python/
|        +--job/
|           +--module/
|           +--tests/
|           +--Dockerfile
|           +--README.md
|           +--requirements.txt
|        +--ci_job.template.yaml
|        +--ci_workflow.template.yaml

```

## Development

The tools in this repository are intended for python 3.8+.

To install dependencies:

```sh
pip install -r requirements.txt
```

This project uses `pip-tools` to pin dependencies.  New dependencies go in
`requirements.in` and `pip-compile` is used to generate `requirements.txt`:

```sh
pip install pip-tools
pip-compile --generate-hashes requirements.in
```

To run tests:

```sh
pytest --flake8 --black tests/
```

### Adding a new job

To add a new job:

```sh
./script/create_job --job-name example-job --template python
```

`job-name` is the name of the directory that will be created in `jobs/`.

`template` is an optional argument that will populate the created directory
with the contents of a template.
If no template is given, a directory with only the required files is created.

#### Available Templates:

| Template name | Description |
| ------------- | ----------- |
| default       | Base directory with readme, Dockerfile, and CI config files |
| python        | Simple Python module with unit test and lint config |

### Modifying the CI config

This repo uses CircleCI which only allows a single global config file.
In order to simplify adding and removing jobs to CI, the config file is 
generated using templates.
This means the `config.yml` in `.circleci/` should not be modified directly.

Generate `.circleci/config.yml`:

```sh
./script/update_ci_config
```

To make changes to the config that are not ETL job specific 
(e.g. add a command), changes should be made to `templates/config.template.yml` 
and the output config should be re-generated.

Each job has a `ci_job.yaml` and a `ci_workflow.yaml` which define the steps 
that will go into the jobs and workflow sections of the CircleCI config.
Any changes to these files should be followed by updating the global config
via `scripts/update_ci_config`.
When a job is created, the CI files are created based on the 
`ci_*.template.yaml` files in the template directory.

### Adding a template

To add a new template, create a new directory in `templates/` with the name
of the template.
This directory must have a `ci_job.template.yaml`, a `ci_workflow.template.yaml`,
and a `job/` directory which contains all the files that will be copied to 
any job that uses this template.
 