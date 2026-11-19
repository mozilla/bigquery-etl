from bigquery_etl.schema import Schema
from sql_generators.mdn_legacy_views import generate_view


def _field(name, type="STRING", fields=None):
    field = {"name": name, "type": type, "mode": "NULLABLE"}
    if fields:
        field["fields"] = fields
    return field


YARI_SCHEMA = Schema(
    {
        "fields": [
            _field("client_info", "RECORD", [_field("client_id"), _field("os")]),
            _field(
                "ping_info",
                "RECORD",
                [
                    _field("reason"),
                    _field(
                        "server_knobs_config",
                        "RECORD",
                        [_field("events_ping_acceleration_factor", "FLOAT")],
                    ),
                ],
            ),
            {**_field("events", "RECORD", [_field("name")]), "mode": "REPEATED"},
            _field("submission_timestamp", "TIMESTAMP"),
        ]
    }
)

# Fred query schema with a different field order and missing Yari fields
FRED_SCHEMA = Schema(
    {
        "fields": [
            _field("submission_timestamp", "TIMESTAMP"),
            _field("ping_info", "RECORD", [_field("reason")]),
            _field("client_info", "RECORD", [_field("client_id")]),
        ]
    }
)


def test_generate_view():
    sql = " ".join(
        generate_view(
            "legacy_page", "moz-fx-data-shared-prod", YARI_SCHEMA, FRED_SCHEMA
        ).split()
    )

    assert "VIEW `moz-fx-data-shared-prod.mdn.legacy_page`" in sql
    assert "AND event_name = 'page_load'" in sql
    fred_select, yari_select = sql.split("UNION ALL")
    # Fred fields follow the Yari field order, missing fields are NULL
    assert fred_select.index("client_info.client_id") < fred_select.index(
        "ping_info.reason"
    )
    assert "CAST(NULL AS STRING) AS `os`" in fred_select
    assert (
        "CAST(NULL AS STRUCT<`events_ping_acceleration_factor` FLOAT64>) "
        "AS `server_knobs_config`"
    ) in fred_select
    # missing repeated fields are empty arrays rather than NULL
    assert "CAST([] AS ARRAY<STRUCT<`name` STRING>>) AS `events`" in fred_select
    # Yari fields are listed explicitly, so upstream additions can't break the view
    assert "SELECT *" not in yari_select
    assert "ping_info.server_knobs_config.events_ping_acceleration_factor" in (
        yari_select
    )
