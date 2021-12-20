from hashlib import sha256

import click
import pytest
import stripe
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
    expect = {
        "created": "2020-01-01 00:00:00",
        "id": "cus_1",
        "metadata": [{"key": "fxa_uid", "value": sha256(b"raw_user_id").hexdigest()}],
    }
    assert filtered_schema.format_row(customer, strict=True) == expect

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
    assert filtered_schema.format_row(dispute, strict=True) == dispute

    filtered_schema = FilteredSchema(stripe.Invoice)
    invoice = {
        "amount_due": 5,
        "custom_fields": [
            {"name": "userid", "value": "raw_user_id"},
        ],
    }
    expect = {
        "amount_due": 5,
        "custom_fields": [
            {"name": "fxa_uid", "value": sha256(b"raw_user_id").hexdigest()}
        ],
    }
    assert filtered_schema.format_row(invoice, strict=True) == expect


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
        filtered_schema.format_row(row, strict=True)
    assert e.value.message == message


def test_nonstrict_mode():
    filtered_schema = FilteredSchema(stripe.Dispute)
    row = {
        "unexpected": "field",
        "amount": "123",
        "balance_transactions": [
            {"exchange_rate": "1.23", "fee": 123},
            {
                "exchange_rate": 1.5,
            },
        ],
    }
    expected = {
        "amount": "123",
        "balance_transactions": [
            {"exchange_rate": "1.23", "fee": 123},
            {
                "exchange_rate": 1.5,
            },
        ],
    }
    assert filtered_schema.format_row(row, strict=False) == expected
