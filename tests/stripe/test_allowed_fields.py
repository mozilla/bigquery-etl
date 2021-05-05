from hashlib import sha256

import click
import pytest
import stripe
import ujson
from google.cloud import bigquery

from bigquery_etl.stripe.allowed_fields import FilteredSchema


def test_filter_schema():
    filtered_schema = FilteredSchema(stripe.Customer)
    assert filtered_schema.filtered == (
        bigquery.SchemaField(name="created", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="id", field_type="STRING"),
        bigquery.SchemaField(
            name="metadata",
            field_type="RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(name="key", field_type="STRING"),
                bigquery.SchemaField(name="value", field_type="STRING"),
            ],
        ),
    )
    customer_schema = filtered_schema.filtered

    event_schema = FilteredSchema(stripe.Event).filtered
    data_schema = next(f.fields for f in event_schema if f.name == "data")
    event_customer_schema = next(f.fields for f in data_schema if f.name == "customer")
    assert customer_schema == event_customer_schema


def test_format_row():
    filtered_schema = FilteredSchema(stripe.Customer)
    customer = {
        "created": "2020-01-01 00:00:00",
        "default_source": "source_1",
        "id": "cus_1",
        "livemode": False,
        "metadata": {"userid": "raw_user_id"},
        "shipping": {
            "address": {"city": None, "country": "US", "postal_code": 12345},
            "name": "James Tiberius Kirk",
            "phone": "555-555-5555",
        },
    }
    expect = ujson.dumps(
        {
            "created": "2020-01-01 00:00:00",
            "id": "cus_1",
            "metadata": [
                {"key": "fxa_uid", "value": sha256(b"raw_user_id").hexdigest()}
            ],
        }
    ).encode("UTF-8")
    assert filtered_schema.format_row(customer) == expect

    filtered_schema = FilteredSchema(stripe.Dispute)
    dispute = {
        "amount": "123",
        "balance_transactions": [
            {"exchange_rate": "1.23", "fee": 123},
            {
                "exchange_rate": 1.5,
            },
        ],
    }
    assert filtered_schema.format_row(dispute) == ujson.dumps(dispute).encode("UTF-8")


@pytest.mark.parametrize(
    "row,message",
    [
        ({"extra field": "value"}, "dispute contained unexpected field: extra field"),
        ({"id": {"": ""}}, "expected STRING at dispute.id but got RECORD"),
        ({"id": [""]}, "expected STRING at dispute.id but got ARRAY"),
        ({"livemode": "false"}, "expected BOOLEAN at dispute.livemode but got 'false'"),
        ({"amount": "1.23"}, "expected INTEGER at dispute.amount but got '1.23'"),
        (
            {"balance_transactions": [{"exchange_rate": "1.23.0"}]},
            "expected NUMERIC at dispute.balance_transactions[0].exchange_rate"
            " but got '1.23.0'",
        ),
        (
            {"balance_transactions": [""]},
            "expected RECORD at dispute.balance_transactions[0] but got ''",
        ),
        (
            {"balance_transactions": {"": ""}},
            "expected ARRAY at dispute.balance_transactions but got RECORD",
        ),
    ],
)
def test_invalid_rows(row, message):
    filtered_schema = FilteredSchema(stripe.Dispute)
    with pytest.raises(click.ClickException) as e:
        filtered_schema.format_row(row)
    assert e.value.message == message
