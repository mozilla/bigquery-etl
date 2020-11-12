from pathlib import Path

import bigquery_etl.query_scheduling.generate_airflow_dags as gad

TEST_DIR = Path(__file__).parent.parent


class TestGenerateAirflowDags(object):
    sql_dir = TEST_DIR / "data" / "test_sql"
    dags_config = TEST_DIR / "data" / "dags.yaml"

    def test_get_dags(self):
        dags = gad.get_dags(self.sql_dir, self.dags_config)

        assert len(dags.dags) == 2
        assert dags.dag_by_name("not existing") is None
        assert dags.dag_by_name("bqetl_events") is not None
        assert dags.dag_by_name("bqetl_core") is not None

        events_dag = dags.dag_by_name("bqetl_events")
        assert len(events_dag.tasks) == 3
        assert events_dag.tasks[0].depends_on_past is False
        assert events_dag.tasks[1].depends_on_past is False

        core_dag = dags.dag_by_name("bqetl_core")
        assert len(core_dag.tasks) == 3
