import copy
from uuid import uuid4

import pytest
import requests
from click.testing import CliRunner
from google.cloud import bigquery

from bigquery_etl.alchemer.survey import (
    construct_data,
    date_plus_one,
    format_responses,
    get_survey_data,
    insert_to_bq,
    main,
    response_schema,
    utc_date_to_eastern_string,
)

# https://apihelp.alchemer.com/help/surveyresponse-returned-fields-v5#getobject
EXAMPLE_RESPONSE = {
    "result_ok": True,
    "total_count": 2,
    "page": 1,
    "total_pages": 1,
    "results_per_page": 50,
    "data": [
        {
            "id": "1",
            "contact_id": "",
            "status": "Complete",
            "is_test_data": "0",
            "date_submitted": "2018-09-27 10:42:26 EDT",
            "session_id": "1538059336_5bacec4869caa2.27680217",
            "language": "English",
            "date_started": "2018-09-27 10:42:16 EDT",
            "link_id": "7473882",
            "url_variables": [],
            "ip_address": "50.232.185.226",
            "referer": "https://app.alchemer.com/distribute/share/id/4599075",
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/69.0.3497.100 Safari/537.36"
            ),
            "response_time": 10,
            "data_quality": [],
            "longitude": "-105.20369720459",
            "latitude": "40.050701141357",
            "country": "United States",
            "city": "Boulder",
            "region": "CO",
            "postal": "80301",
            "dma": "751",
            "survey_data": {
                "2": {
                    "id": 2,
                    "type": "RADIO",
                    "question": "Will you attend the event?",
                    "section_id": 1,
                    "original_answer": "Yes",
                    "answer": "1",
                    "answer_id": 10001,
                    "shown": True,
                },
                "3": {
                    "id": 3,
                    "type": "TEXTBOX",
                    "question": "How many guests will you bring?",
                    "section_id": 1,
                    "answer": "3",
                    "shown": True,
                },
                "4": {
                    "id": 4,
                    "type": "TEXTBOX",
                    "question": "How many guests are under the age of 18?",
                    "section_id": 1,
                    "answer": "2",
                    "shown": True,
                },
            },
        },
        {
            "id": "2",
            "contact_id": "",
            "status": "Complete",
            "is_test_data": "0",
            "date_submitted": "2018-09-27 10:43:11 EDT",
            "session_id": "1538059381_5bacec751e41f4.51482165",
            "language": "English",
            "date_started": "2018-09-27 10:43:01 EDT",
            "link_id": "7473882",
            "url_variables": {
                "__dbget": {"key": "__dbget", "value": "true", "type": "url"}
            },
            "ip_address": "50.232.185.226",
            "referer": "",
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/69.0.3497.100 Safari/537.36"
            ),
            "response_time": 10,
            "data_quality": [],
            "longitude": "-105.20369720459",
            "latitude": "40.050701141357",
            "country": "United States",
            "city": "Boulder",
            "region": "CO",
            "postal": "80301",
            "dma": "751",
            "survey_data": {
                "2": {
                    "id": 2,
                    "type": "RADIO",
                    "question": "Will you attend the event?",
                    "section_id": 1,
                    "original_answer": "1",
                    "answer": "1",
                    "answer_id": 10001,
                    "shown": True,
                },
                "3": {
                    "id": 3,
                    "type": "TEXTBOX",
                    "question": "How many guests will you bring?",
                    "section_id": 1,
                    "answer": "2",
                    "shown": True,
                },
                "4": {
                    "id": 4,
                    "type": "TEXTBOX",
                    "question": "How many guests are under the age of 18?",
                    "section_id": 1,
                    "answer": "0",
                    "shown": True,
                },
            },
        },
    ],
}

SUBMISSION_DATE = "2021-01-05"

EXAMPLE_RESPONSE_FORMATTED_0 = {
    "submission_date": SUBMISSION_DATE,
    "id": "1",
    "status": "Complete",
    "session_id": "1538059336_5bacec4869caa2.27680217",
    "response_time": 10,
    "survey_data": [
        {
            "id": 2,
            "type": "RADIO",
            "question": "Will you attend the event?",
            "section_id": 1,
            "original_answer": "Yes",
            "answer": "1",
            "answer_id": 10001,
            "shown": True,
        },
        {
            "id": 3,
            "type": "TEXTBOX",
            "question": "How many guests will you bring?",
            "section_id": 1,
            "answer": "3",
            "shown": True,
        },
        {
            "id": 4,
            "type": "TEXTBOX",
            "question": "How many guests are under the age of 18?",
            "section_id": 1,
            "answer": "2",
            "shown": True,
        },
    ],
}

EXAMPLE_RESPONSE_FORMATTED = [
    EXAMPLE_RESPONSE_FORMATTED_0,
    {
        "submission_date": SUBMISSION_DATE,
        "id": "2",
        "status": "Complete",
        "session_id": "1538059381_5bacec751e41f4.51482165",
        "response_time": 10,
        "survey_data": [
            {
                "id": 2,
                "type": "RADIO",
                "question": "Will you attend the event?",
                "section_id": 1,
                "original_answer": "1",
                "answer": "1",
                "answer_id": 10001,
                "shown": True,
            },
            {
                "id": 3,
                "type": "TEXTBOX",
                "question": "How many guests will you bring?",
                "section_id": 1,
                "answer": "2",
                "shown": True,
            },
            {
                "id": 4,
                "type": "TEXTBOX",
                "question": "How many guests are under the age of 18?",
                "section_id": 1,
                "answer": "0",
                "shown": True,
            },
        ],
    },
]


@pytest.fixture()
def testing_client():
    bq = bigquery.Client()
    yield bq


@pytest.fixture()
def testing_dataset(testing_client):
    bq = testing_client
    dataset_id = f"test_survey_pytest_{str(uuid4())[:8]}"
    bq.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    dataset = bq.create_dataset(dataset_id)
    yield dataset
    bq.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.fixture()
def testing_table_id(testing_dataset):
    table_ref = testing_dataset.table(f"survey_testing_table_{str(uuid4())[:8]}")
    table_id = f"{table_ref.dataset_id}.{table_ref.table_id}"
    yield table_id


@pytest.fixture()
def patch_api_requests(monkeypatch):
    # Note: this does not test iterating over multiple pages
    class MockResponse:
        @staticmethod
        def raise_for_status():
            pass

        @staticmethod
        def json():
            return EXAMPLE_RESPONSE

    def mock_get(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(requests, "get", mock_get)


def test_utc_date_to_eastern_time():
    # UTC-5 during standard time: https://en.wikipedia.org/wiki/Eastern_Time_Zone
    assert utc_date_to_eastern_string("2021-01-05") == "2021-01-04+19:00:00"


def test_date_plus_one():
    assert date_plus_one("2020-01-05") == "2020-01-06"


def test_format_response():
    assert (
        format_responses(EXAMPLE_RESPONSE["data"][0], SUBMISSION_DATE)
        == EXAMPLE_RESPONSE_FORMATTED_0
    )


def test_format_response_nonnumeric_answer_id():
    base = {
        "submission_date": SUBMISSION_DATE,
        "id": "1",
        "status": "Complete",
        "session_id": "1538059336_5bacec4869caa2.27680217",
        "response_time": 10,
        "survey_data": {
            "1": {
                "answer_id": "10001-other",
            },
            "2": {
                "answer_id": "fadfasdf-other",
            },
        },
    }
    res = format_responses(base, SUBMISSION_DATE)
    assert res["survey_data"][0]["answer_id"] == 10001
    assert not res["survey_data"][1].get("answer_id")


def test_construct_data():
    assert (
        construct_data(EXAMPLE_RESPONSE, SUBMISSION_DATE) == EXAMPLE_RESPONSE_FORMATTED
    )


def test_get_survey_data(patch_api_requests):
    assert (
        get_survey_data("555555", SUBMISSION_DATE, "token", "secret")
        == EXAMPLE_RESPONSE_FORMATTED
    )


def test_response_schema():
    # ensure that there aren't any exceptions
    assert response_schema()


def test_insert_to_bq(testing_table_id):
    transformed = construct_data(EXAMPLE_RESPONSE, SUBMISSION_DATE)
    insert_to_bq(transformed, testing_table_id, SUBMISSION_DATE)


def test_insert_to_bq_options(testing_table_id):
    # Override survey data, but make sure to deep copy to prevent mutating state
    # in other tests.
    # https://apihelp.alchemer.com/help/surveyresponse-per-question-v5#textboxlist
    base = copy.deepcopy(EXAMPLE_RESPONSE["data"][0])
    base["survey_data"] = {
        "37": {
            "id": 37,
            "type": "parent",
            "question": "Textbox List Question Title",
            "section_id": 3,
            "options": {
                "10068": {"id": 10068, "option": "Row 1", "answer": "text list answer"}
            },
            "shown": True,
        },
        "38": {
            "id": 38,
            "type": "parent",
            "question": "Continuous Sum Question Title",
            "section_id": 3,
            "options": {
                "10070": {"id": 10070, "option": "Row 1", "answer": "6"},
                "10071": {"id": 10071, "option": "Row 2", "answer": "7"},
            },
            "shown": True,
        },
    }
    transformed = [format_responses(base, SUBMISSION_DATE)]
    insert_to_bq(transformed, testing_table_id, SUBMISSION_DATE)


def test_insert_to_bq_subquestions(testing_table_id):
    # Override survey data. Note that the subquestion object is incompatible.
    # https://apihelp.alchemer.com/help/surveyresponse-per-question-v5#checkboxgrid
    base = copy.deepcopy(EXAMPLE_RESPONSE["data"][0])
    base["survey_data"] = {
        "30": {
            "id": 30,
            "type": "parent",
            "question": "Checkbox Grid Question Title",
            "subquestions": {
                "31": {
                    "10062": {
                        "id": 10062,
                        "type": "CHECKBOX",
                        "parent": 30,
                        "question": "Row 1 : Column 1",
                        "answer": "Column 1",
                        "shown": True,
                    },
                    "10063": {
                        "id": 10063,
                        "type": "CHECKBOX",
                        "parent": 30,
                        "question": "Row 1 : Column 2",
                        "answer": None,
                        "shown": True,
                    },
                },
                "32": {
                    "10062": {
                        "id": 10062,
                        "type": "CHECKBOX",
                        "parent": 30,
                        "question": "Row 2 : Column 1",
                        "answer": None,
                        "shown": True,
                    },
                    "10063": {
                        "id": 10063,
                        "type": "CHECKBOX",
                        "parent": 30,
                        "question": "Row 2 : Column 2",
                        "answer": "Column 2",
                        "shown": True,
                    },
                },
            },
            "section_id": 3,
            "shown": True,
        },
        "83": {
            "id": 83,
            "type": "parent",
            "question": "Custom Table Question Title",
            "subquestions": {
                "10001": {
                    "id": 10001,
                    "type": "RADIO",
                    "question": "Radio Button Column",
                    "section_id": 4,
                    "answer": "Option 1",
                    "answer_id": 10113,
                    "shown": True,
                },
                "10002": {
                    "id": 10002,
                    "type": "RADIO",
                    "question": "Radio Button Column",
                    "section_id": 4,
                    "answer": "Option 2",
                    "answer_id": 10114,
                    "shown": True,
                },
            },
            "section_id": 4,
            "shown": True,
        },
    }
    transformed = [format_responses(base, SUBMISSION_DATE)]
    insert_to_bq(transformed, testing_table_id, SUBMISSION_DATE)


def test_cli(patch_api_requests, testing_table_id):
    res = CliRunner().invoke(
        main,
        [
            "--date",
            SUBMISSION_DATE,
            "--survey_id",
            "55555",
            "--api_token",
            "token",
            "--api_secret",
            "secret",
            "--destination_table",
            testing_table_id,
        ],
        catch_exceptions=False,
    )
    assert res.exit_code == 0
