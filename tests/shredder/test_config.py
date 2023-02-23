from multiprocessing.pool import ThreadPool

from google.cloud import bigquery

from bigquery_etl.shredder.config import DeleteSource, DeleteTarget, find_glean_targets


class FakeClient:
    def list_datasets(self, project):
        return [
            bigquery.DatasetReference(project, f"{app}_{suffix}")
            for app in ["fenix", "focus_android"]
            for suffix in ["stable", "derived"]
        ]

    def list_tables(self, dataset_ref):
        labels = {}
        if dataset_ref.dataset_id.endswith("stable"):
            table_ids = ["metrics_v1", "deletion_request_v1", "migration_v1"]
            labels["schema_id"] = "glean_ping_1"
        elif dataset_ref.dataset_id == "focus_android_derived":
            table_ids = [
                "additional_deletion_requests_v1",
                "clients_daily_v1",
                "dau_v1",
            ]
        elif dataset_ref.dataset_id.endswith("derived"):
            table_ids = ["clients_daily_v1"]
        else:
            raise Exception(f"unexpected dataset: {dataset_ref}")
        return [
            bigquery.table.TableListItem(
                {
                    "tableReference": bigquery.TableReference(
                        dataset_ref, table_id
                    ).to_api_repr(),
                    "labels": labels,
                }
            )
            for table_id in table_ids
        ]

    def get_table(self, table_ref):
        table = bigquery.Table(table_ref)
        if table_ref.dataset_id.endswith("stable"):
            table.schema = [
                bigquery.SchemaField(
                    "client_info",
                    "RECORD",
                    "NULLABLE",
                    [bigquery.SchemaField("client_id", "STRING")],
                )
            ]
        elif table_ref.table_id in {
            "additional_deletion_requests_v1",
            "clients_daily_v1",
        }:
            table.schema = [bigquery.SchemaField("client_id", "STRING")]
        else:
            table.schema = [bigquery.SchemaField("document_id", "STRING")]
        return table


def test_glean_targets():
    with ThreadPool(1) as pool:
        targets = find_glean_targets(pool, FakeClient())
    assert targets == {
        **{
            target: (
                DeleteSource(
                    table="fenix_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            )
            for target in [
                DeleteTarget(
                    table="fenix_stable.metrics_v1",
                    field=("client_info.client_id",),
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="fenix_derived.clients_daily_v1",
                    field=("client_id",),
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
        **{
            target: (
                DeleteSource(
                    table="focus_android_stable.deletion_request_v1",
                    field="client_info.client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
                DeleteSource(
                    table="focus_android_derived.additional_deletion_requests_v1",
                    field="client_id",
                    project="moz-fx-data-shared-prod",
                    conditions=(),
                ),
            )
            for target in [
                DeleteTarget(
                    table="focus_android_stable.metrics_v1",
                    field=("client_info.client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
                DeleteTarget(
                    table="focus_android_derived.clients_daily_v1",
                    field=("client_id",) * 2,
                    project="moz-fx-data-shared-prod",
                ),
            ]
        },
    }
