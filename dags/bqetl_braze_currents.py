# Generated via https://github.com/mozilla/bigquery-etl/blob/main/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.gcp import bigquery_etl_query, bigquery_dq_check, bigquery_bigeye_check

docs = """
### bqetl_braze_currents

Built from bigquery-etl repo, [`dags/bqetl_braze_currents.py`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_braze_currents.py)

#### Description

Load Braze current data from GCS into BigQuery

#### Owner

cbeck@mozilla.com

#### Tags

* impact/tier_2
* repo/bigquery-etl
"""


default_args = {
    "owner": "cbeck@mozilla.com",
    "start_date": datetime.datetime(2024, 4, 15, 0, 0),
    "end_date": None,
    "email": ["cbeck@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
}

tags = ["impact/tier_2", "repo/bigquery-etl"]

with DAG(
    "bqetl_braze_currents",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    doc_md=docs,
    tags=tags,
    catchup=False,
) as dag:

    braze_external__braze_currents_firefox_click__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_click__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_click_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_click_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Click",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_conversion__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_conversion__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_conversion_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_conversion_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.campaigns.Conversion",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_delivery__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_delivery__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_delivery_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_delivery_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Delivery",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_global_state_changes__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_global_state_changes__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_global_state_changes_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_global_state_changes_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.behaviors.subscription.GlobalStateChange",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_hard_bounces__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_hard_bounces__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_hard_bounces_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_hard_bounces_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Bounce",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    with TaskGroup(
        "braze_external__braze_currents_firefox_hard_bounces__v1_external",
    ) as braze_external__braze_currents_firefox_hard_bounces__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_marketing_suppression_list__wait_for_braze_external__braze_currents_firefox_hard_bounces__v1",
            external_dag_id="bqetl_marketing_suppression_list",
            external_task_id="wait_for_braze_external__braze_currents_firefox_hard_bounces__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        braze_external__braze_currents_firefox_hard_bounces__v1_external.set_upstream(
            braze_external__braze_currents_firefox_hard_bounces__v1
        )

    braze_external__braze_currents_firefox_mark_as_spam__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_mark_as_spam__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_mark_as_spam_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_mark_as_spam_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.MarkAsSpam",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_open__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_open__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_open_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_open_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Open",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_send__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_send__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_send_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_send_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Send",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_soft_bounce__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_soft_bounce__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_soft_bounce_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_soft_bounce_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.SoftBounce",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_state_changes__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_state_changes__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_state_changes_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_state_changes_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.behaviors.subscriptiongroup.StateChange",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_firefox_unsubscribe__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_firefox_unsubscribe__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_firefox_unsubscribe_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_firefox_unsubscribe_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-firefox",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf55eea9932004d7fb071",
            "--event-type=users.messages.email.Unsubscribe",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    with TaskGroup(
        "braze_external__braze_currents_firefox_unsubscribe__v1_external",
    ) as braze_external__braze_currents_firefox_unsubscribe__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_marketing_suppression_list__wait_for_braze_external__braze_currents_firefox_unsubscribe__v1",
            external_dag_id="bqetl_marketing_suppression_list",
            external_task_id="wait_for_braze_external__braze_currents_firefox_unsubscribe__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        braze_external__braze_currents_firefox_unsubscribe__v1_external.set_upstream(
            braze_external__braze_currents_firefox_unsubscribe__v1
        )

    braze_external__braze_currents_mozilla_click__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_click__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_click_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_click_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.Click",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_conversion__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_conversion__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_conversion_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_conversion_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.campaigns.Conversion",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_delivery__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_delivery__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_delivery_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_delivery_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.Delivery",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_global_state_changes__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_global_state_changes__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_global_state_changes_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_global_state_changes_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.behaviors.subscription.GlobalStateChange",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_hard_bounces__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_hard_bounces__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_hard_bounces_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_hard_bounces_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.Bounce",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    with TaskGroup(
        "braze_external__braze_currents_mozilla_hard_bounces__v1_external",
    ) as braze_external__braze_currents_mozilla_hard_bounces__v1_external:
        ExternalTaskMarker(
            task_id="bqetl_marketing_suppression_list__wait_for_braze_external__braze_currents_mozilla_hard_bounces__v1",
            external_dag_id="bqetl_marketing_suppression_list",
            external_task_id="wait_for_braze_external__braze_currents_mozilla_hard_bounces__v1",
            execution_date="{{ (execution_date - macros.timedelta(days=-1, seconds=82800)).isoformat() }}",
        )

        braze_external__braze_currents_mozilla_hard_bounces__v1_external.set_upstream(
            braze_external__braze_currents_mozilla_hard_bounces__v1
        )

    braze_external__braze_currents_mozilla_mark_as_spam__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_mark_as_spam__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_mark_as_spam_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_mark_as_spam_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.MarkAsSpam",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_open__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_open__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_open_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_open_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.Open",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_send__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_send__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_send_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_send_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.Send",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_soft_bounce__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_soft_bounce__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_soft_bounce_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_soft_bounce_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.messages.email.SoftBounce",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )

    braze_external__braze_currents_mozilla_state_changes__v1 = GKEPodOperator(
        task_id="braze_external__braze_currents_mozilla_state_changes__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/braze_external/braze_currents_mozilla_state_changes_v1/query.py",
        ]
        + [
            "--destination-project=moz-fx-data-shared-prod",
            "--destination-dataset=braze_external",
            "--destination-table=braze_currents_mozilla_state_changes_v1",
            "--source-bucket=moz-fx-data-marketing-prod-braze-mozilla",
            "--source-prefix=currents/dataexport.prod-05.GCS.integration.65fdf62e352dc7004cebd366",
            "--event-type=users.behaviors.subscriptiongroup.StateChange",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        owner="cbeck@mozilla.com",
        email=["cbeck@mozilla.com"],
    )
