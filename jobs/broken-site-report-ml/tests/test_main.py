from unittest import TestCase
from unittest.mock import Mock, patch
from broken_site_report_ml.main import add_classification_results
from broken_site_report_ml.main import record_classification_run
from broken_site_report_ml.main import get_reports_classification
from broken_site_report_ml.main import deduplicate_reports
from broken_site_report_ml.main import translate_reports
from google.cloud.bigquery import Row


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
        mock_client.get_table.assert_called_with(f"{bq_dataset_id}.bugbug_predictions")

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


class TestDeduplicateTranslateReports(TestCase):
    def setUp(self):
        self.schema = {"uuid": 0, "body": 1, "title": 2, "translated_text": 3}
        self.reports = [
            Row(
                (
                    "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                    "test message",
                    "https://example.com",
                    "",
                ),
                self.schema,
            ),
            Row(
                (
                    "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                    "test message",
                    "https://example.com",
                    "",
                ),
                self.schema,
            ),
            Row(
                (
                    "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
                    "navegador no compatible",
                    "https://example.com/es",
                    "",
                ),
                self.schema,
            ),
            Row(
                (
                    "ww1958d4-440f-4479-b8b9-2e60b76427daa",
                    "图片记载不出来，搜索框无法使用\n",
                    "https://example.com",
                    "The picture cannot be recorded\n",
                ),
                self.schema,
            ),
        ]

    def test_deduplication(self):
        deduplicated = deduplicate_reports(self.reports)
        expected = [
            {
                "uuid": "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                "body": "test message",
                "title": "https://example.com",
                "translated_text": "",
            },
            {
                "uuid": "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
                "body": "navegador no compatible",
                "title": "https://example.com/es",
                "translated_text": "",
            },
            {
                "uuid": "ww1958d4-440f-4479-b8b9-2e60b76427daa",
                "body": "图片记载不出来，搜索框无法使用\n",
                "title": "https://example.com",
                "translated_text": "The picture cannot be recorded\n",
            },
        ]
        self.assertEqual(len(deduplicated), 3)
        self.assertEqual(deduplicate_reports(self.reports), expected)

    def test_empty_list(self):
        deduplicated = deduplicate_reports([])
        self.assertEqual(len(deduplicated), 0, "Should handle empty list without error")

    @patch("broken_site_report_ml.main.bigquery.Client")
    @patch("broken_site_report_ml.main.translate_by_uuid")
    def test_translate_reports(self, mock_translate_by_uuid, mock_bq_client):
        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        schema = {"uuid": 0, "language_code": 1, "translated_text": 2, "status": 3}
        mock_translate_by_uuid.return_value = [
            Row(
                ("bc1758d4-880f-4b79-b8b9-2e60b76427daa", "en", "test message", ""),
                schema,
            ),
            Row(
                (
                    "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
                    "es",
                    "navegador no compatible",
                    "",
                ),
                schema,
            ),
        ]

        mock_bq_dataset_id = "test_dataset"
        reports = [
            {
                "uuid": "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                "body": "test message",
                "title": "https://example.com",
                "translated_text": "",
            },
            {
                "uuid": "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
                "body": "navegador no compatible",
                "title": "https://example.com/es",
                "translated_text": "",
            },
            {
                "uuid": "ww1958d4-440f-4479-b8b9-2e60b76427daa",
                "body": "图片记载不出来，搜索框无法使用\n",
                "title": "https://example.com",
                "translated_text": "The picture cannot be recorded\n",
            },
        ]

        expected_uuids = [
            "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
            "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
        ]
        result = translate_reports(mock_client, reports, mock_bq_dataset_id)

        mock_translate_by_uuid.assert_called_once_with(
            mock_client, expected_uuids, mock_bq_dataset_id
        )
        self.assertEqual(
            result,
            {
                "bc1758d4-880f-4b79-b8b9-2e60b76427daa": {
                    "uuid": "bc1758d4-880f-4b79-b8b9-2e60b76427daa",
                    "language_code": "en",
                    "translated_text": "test message",
                    "status": "",
                },
                "ac1958d4-880f-4b79-b8b9-2e60b76427daa": {
                    "uuid": "ac1958d4-880f-4b79-b8b9-2e60b76427daa",
                    "language_code": "es",
                    "translated_text": "navegador no compatible",
                    "status": "",
                },
            },
        )
