from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).parent.parent.parent


def test_dag_names():
    """Verify dag names.

    DAG names defined in this repository must start with `bqetl_`.
    CLI allows to create DAGs prefixed with `bqetl_` and `private_bqetl_`,
    the latter are reserved for private version of bigquery-etl repository.
    """
    with open(str(ROOT_DIR / "dags.yaml"), "r") as dags_file:
        dags_conf = yaml.safe_load(dags_file.read())
    for dag_name in dags_conf:
        assert dag_name.startswith("bqetl_")
