from unittest import TestCase
from unittest.mock import Mock, patch
from broken_site_report_ml.main import add_classification_results
from broken_site_report_ml.main import record_classification_run
from broken_site_report_ml.main import get_reports_classification


class TestMain(TestCase):
    @patch("broken_site_report_ml.main.bigquery.Client")
    def test_add_classification_results(self, mock_bq_client):
        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        mock_job = Mock()
        mock_client.load_table_from_json.return_value = mock_job
        mock_job.result.return_value = None

        mock_table = Mock()
        mock_client.get_table.return_value = mock_table

        bq_dataset_id = "test_dataset"
        results = {
            "bc1758d4-880f-4b79-b8b9-2e60b76427daa": {
                "prob": [0.07616984844207764, 0.9238301515579224],
                "index": 1,
                "class": 1,
                "extra_data": {},
            }
        }

        add_classification_results(mock_client, bq_dataset_id, results)

        mock_client.load_table_from_json.assert_called_once()
        mock_client.get_table.assert_called_with(f"{bq_dataset_id}.labels")

    @patch("broken_site_report_ml.main.bigquery.Client")
    def test_record_classification_run(self, mock_bq_client):
        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        bq_dataset_id = "test_dataset"
        is_ok = True
        count = 10

        record_classification_run(mock_client, bq_dataset_id, is_ok, count)
        mock_client.insert_rows_json.assert_called_once()

    @patch("broken_site_report_ml.main.classification_http_request")
    @patch("broken_site_report_ml.main.time.sleep", Mock())
    def test_get_reports_classification(self, mock_classification_http_request):
        report_uuid = "bc1758d4-880f-4b79-b8b9-2e60b76427daa"
        classification_result = {
            "prob": [0.07616984844207764, 0.9238301515579224],
            "index": 1,
            "class": 1,
            "extra_data": {},
        }
        mock_response = {"reports": {report_uuid: classification_result}}
        mock_classification_http_request.return_value = mock_response

        model = "test_model"
        reports = {
            report_uuid: {
                "uuid": "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                "title": "https://example.com",
                "body": "Doesn't load",
            }
        }

        response = get_reports_classification(model, reports)
        self.assertEqual(
            response,
            {
                report_uuid: {
                    "prob": [0.07616984844207764, 0.9238301515579224],
                    "index": 1,
                    "class": 1,
                    "extra_data": {},
                }
            },
        )
        self.assertEqual(mock_classification_http_request.call_count, 1)
