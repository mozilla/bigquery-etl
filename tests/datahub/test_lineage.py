import time
from unittest.mock import MagicMock, patch

import click
import pytest
import requests

from bigquery_etl.datahub.lineage import (
    AuthError,
    DataHubClient,
    LineageTooLargeError,
    format_text,
    get_downstream_lineage,
    is_stale_looker_tmp,
    qualified_name,
    sql_path,
    table_urn,
    to_asset,
)


def result(urn, entity_type, degree=1, platform="bigquery", removed=False, **extra):
    """Build a searchAcrossLineage result like DataHub returns."""
    entity = {
        "urn": urn,
        "type": entity_type,
        "platform": {"name": platform},
        "status": {"removed": removed},
        **extra,
    }
    return {"degree": degree, "entity": entity}


def response(status=200, data=None, errors=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {"data": data, **({"errors": errors} if errors else {})}
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def lineage_page(results, start=0, total=None):
    return {
        "searchAcrossLineage": {
            "start": start,
            "count": len(results),
            "total": len(results) if total is None else total,
            "searchResults": results,
        }
    }


class TestQualifiedName:
    def test_bigquery_uses_qualified_name(self):
        entity = result(
            table_urn("p.d.t"),
            "DATASET",
            properties={"name": "t", "qualifiedName": "p.d.t"},
        )["entity"]
        assert qualified_name(entity) == "p.d.t"

    def test_looker_view_is_project_and_file_path(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:looker,looker-hub.firefox_okrs.views.desktop_retention.view.desktop_retention,PROD)"
        entity = result(urn, "DATASET", platform="looker")["entity"]
        assert (
            qualified_name(entity) == "looker-hub.firefox_okrs.views.desktop_retention"
        )

    def test_looker_explore_is_model_and_explore(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:looker,revenue.explore.search_revenue_levers,PROD)"
        entity = result(urn, "DATASET", platform="looker")["entity"]
        assert qualified_name(entity) == "revenue.search_revenue_levers"

    def test_looker_dashboard_is_folder_path_and_title(self):
        entity = result(
            "urn:li:dashboard:(looker,dashboards.1)",
            "DASHBOARD",
            platform="looker",
            properties={"name": "Search OKRs"},
            browsePathV2={
                "path": [
                    {"name": "Folders", "entity": None},
                    {"name": "c1", "entity": {"properties": {"name": "Shared"}}},
                    {"name": "c2", "entity": {"properties": {"name": "Search"}}},
                ]
            },
        )["entity"]
        assert qualified_name(entity) == "Shared / Search / Search OKRs"

    def test_redash_chart_keeps_its_title(self):
        entity = result(
            "urn:li:chart:(redash,1)",
            "CHART",
            platform="redash",
            properties={"name": "Retention Table"},
            browsePathV2={"path": [{"name": "Default", "entity": None}]},
        )["entity"]
        assert qualified_name(entity) == "Retention Table"


class TestSqlPath:
    @pytest.fixture
    def sql_dir(self, tmp_path):
        for table in ["telemetry_derived/a_v1", "telemetry/a"]:
            (tmp_path / "moz-fx-data-shared-prod" / table).mkdir(parents=True)
        return tmp_path

    def asset(self, name, entity_type="DATASET", platform="bigquery"):
        return to_asset(
            result(
                table_urn(name),
                entity_type,
                platform=platform,
                properties={"qualifiedName": name},
            )
        )

    def test_table_in_sql_dir(self, sql_dir):
        asset = self.asset("moz-fx-data-shared-prod.telemetry_derived.a_v1")
        assert (
            sql_path(asset, sql_dir)
            == sql_dir / "moz-fx-data-shared-prod" / "telemetry_derived" / "a_v1"
        )

    def test_user_facing_view_maps_to_its_source(self, sql_dir):
        asset = self.asset("mozdata.telemetry.a")
        assert (
            sql_path(asset, sql_dir)
            == sql_dir / "moz-fx-data-shared-prod" / "telemetry" / "a"
        )

    @pytest.mark.parametrize(
        "name",
        [
            "moz-fx-data-shared-prod.operational_monitoring_derived.x_v1",
            "mozdata.analysis.x_v1",
            "other-project.telemetry_derived.a_v1",
        ],
    )
    def test_table_outside_sql_dir(self, sql_dir, name):
        assert sql_path(self.asset(name), sql_dir) is None

    def test_other_platforms(self, sql_dir):
        name = "moz-fx-data-shared-prod.telemetry_derived.a_v1"
        assert sql_path(self.asset(name, platform="looker"), sql_dir) is None

    def test_name_without_three_parts(self, sql_dir):
        assert sql_path(self.asset("telemetry_derived.a_v1"), sql_dir) is None


class TestStaleLookerTmp:
    def test_old_copy_is_stale(self):
        urn = table_urn("mozdata.looker_tmp.LR_4M91H1700000000000_rollup")
        assert is_stale_looker_tmp(urn, cutoff_ms=1800000000000)

    def test_recent_copy_is_kept(self):
        urn = table_urn("mozdata.looker_tmp.LR_4M91H1900000000000_rollup")
        assert not is_stale_looker_tmp(urn, cutoff_ms=1800000000000)

    def test_other_tables_are_kept(self):
        urn = table_urn("moz-fx-data-shared-prod.telemetry.LR_4M91H1700000000000_x")
        assert not is_stale_looker_tmp(urn, cutoff_ms=1800000000000)

    def test_no_cutoff_keeps_everything(self):
        urn = table_urn("mozdata.looker_tmp.LR_4M91H1700000000000_rollup")
        assert not is_stale_looker_tmp(urn, cutoff_ms=None)


class TestDataHubClient:
    @pytest.fixture
    def client(self):
        return DataHubClient(token="a-token", url="https://datahub.example")

    @patch("bigquery_etl.datahub.lineage.auth.get_token", return_value="signed-in")
    def test_signs_in_without_a_token(self, get_token, monkeypatch):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
        client = DataHubClient(url="https://datahub.example")
        get_token.assert_called_once_with("https://datahub.example")
        assert client.session.headers["Authorization"] == "Bearer signed-in"

    @patch("bigquery_etl.datahub.lineage.auth.get_token")
    def test_env_token_skips_sign_in(self, get_token, monkeypatch):
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
        DataHubClient(url="https://datahub.example")
        get_token.assert_not_called()

    @patch("bigquery_etl.datahub.lineage.auth.clear_tokens")
    @patch("bigquery_etl.datahub.lineage.auth.get_token")
    def test_rejected_sign_in_signs_in_again_once(
        self, get_token, clear_tokens, monkeypatch
    ):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
        get_token.side_effect = ["revoked", "fresh"]
        client = DataHubClient(url="https://datahub.example")
        client.session.post = MagicMock(
            side_effect=[response(status=401), response(data={"ok": True})]
        )
        assert client.query("q", {}) == {"ok": True}
        clear_tokens.assert_called_once_with("https://datahub.example")
        assert client.session.headers["Authorization"] == "Bearer fresh"

    @patch("bigquery_etl.datahub.lineage.auth.clear_tokens")
    @patch("bigquery_etl.datahub.lineage.auth.get_token")
    def test_rejected_second_sign_in_raises(self, get_token, clear_tokens, monkeypatch):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
        get_token.side_effect = ["revoked", "also-rejected"]
        client = DataHubClient(url="https://datahub.example")
        client.session.post = MagicMock(return_value=response(status=401))
        with pytest.raises(AuthError):
            client.query("q", {})
        assert client.session.post.call_count == 2
        assert get_token.call_count == 2

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    @patch("bigquery_etl.datahub.lineage.auth.clear_tokens")
    @patch("bigquery_etl.datahub.lineage.auth.get_token")
    def test_sign_in_on_the_last_attempt_is_used(
        self, get_token, clear_tokens, sleep, monkeypatch
    ):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
        get_token.side_effect = ["revoked", "fresh"]
        client = DataHubClient(url="https://datahub.example")
        client.session.post = MagicMock(
            side_effect=[response(status=503)] * 4
            + [response(status=401), response(data={"ok": True})]
        )
        assert client.query("q", {}) == {"ok": True}

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_rejected_env_token_is_not_retried(self, sleep, monkeypatch):
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "bad")
        client = DataHubClient(url="https://datahub.example")
        client.session.post = MagicMock(return_value=response(status=401))
        with pytest.raises(AuthError) as e:
            client.query("q", {})
        assert client.session.post.call_count == 1
        assert isinstance(e.value, requests.HTTPError)
        assert e.value.response.status_code == 401

    def test_reads_token_and_url_from_environment(self, monkeypatch):
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
        monkeypatch.setenv("DATAHUB_GMS_URL", "https://other.example/")
        client = DataHubClient()
        assert client.endpoint == "https://other.example/api/graphql"
        assert client.session.headers["Authorization"] == "Bearer env-token"

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_retries_graphql_errors(self, sleep, client):
        client.session.post = MagicMock(
            side_effect=[
                response(errors=[{"message": "Connection lease request time out"}]),
                response(data={"ok": True}),
            ]
        )
        assert client.query("q", {}) == {"ok": True}
        assert client.session.post.call_count == 2

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_gives_up_on_graphql_errors_after_the_last_attempt(self, sleep, client):
        client.session.post = MagicMock(
            return_value=response(
                errors=[{"message": "Connection lease request time out"}]
            )
        )
        with pytest.raises(RuntimeError, match="Connection lease"):
            client.query("q", {})
        assert client.session.post.call_count == 5

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_does_not_retry_other_graphql_errors(self, sleep, client):
        client.session.post = MagicMock(
            return_value=response(
                errors=[{"message": "Unauthorized to perform this action"}]
            )
        )
        with pytest.raises(RuntimeError, match="Unauthorized"):
            client.query("q", {})
        assert client.session.post.call_count == 1
        sleep.assert_not_called()

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_does_not_retry_client_errors(self, sleep, client):
        client.session.post = MagicMock(return_value=response(status=400))
        with pytest.raises(requests.HTTPError):
            client.query("q", {})
        assert client.session.post.call_count == 1

    @pytest.mark.parametrize("status", [429, 502, 503])
    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_retries_overload_statuses(self, sleep, client, status):
        client.session.post = MagicMock(
            side_effect=[response(status=status), response(data={"ok": True})]
        )
        assert client.query("q", {}) == {"ok": True}

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_gives_up_on_overload_statuses_after_the_last_attempt(self, sleep, client):
        client.session.post = MagicMock(return_value=response(status=503))
        with pytest.raises(requests.HTTPError):
            client.query("q", {})
        assert client.session.post.call_count == 5

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_retries_connection_errors_then_gives_up(self, sleep, client):
        client.session.post = MagicMock(side_effect=requests.ConnectionError("down"))
        with pytest.raises(requests.ConnectionError):
            client.query("q", {})
        assert client.session.post.call_count == 5

    def test_response_without_data_is_an_error(self, client):
        client.session.post = MagicMock(return_value=response(data=None))
        with pytest.raises(RuntimeError, match="No data"):
            client.query("q", {})

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_does_not_retry_too_large_lineage(self, sleep, client):
        client.session.post = MagicMock(
            return_value=response(errors=[{"message": "exceeded maxRelations limit"}])
        )
        with pytest.raises(LineageTooLargeError):
            client.query("q", {})
        assert client.session.post.call_count == 1

    def test_search_downstream_pages_and_dedupes(self, client):
        a = result(table_urn("p.d.a"), "DATASET", degree=1)
        b_far = result(table_urn("p.d.b"), "DATASET", degree=3)
        b_near = result(table_urn("p.d.b"), "DATASET", degree=2)
        client.query = MagicMock(
            side_effect=[
                lineage_page([a, b_far], start=0, total=3),
                lineage_page([b_near], start=2, total=3),
            ]
        )
        assets = client.search_downstream(table_urn("p.d.t"))
        assert [(x["urn"], x["degree"]) for x in assets] == [
            (table_urn("p.d.a"), 1),
            (table_urn("p.d.b"), 2),
        ]

    def test_search_downstream_stops_on_an_empty_page(self, client):
        a = result(table_urn("p.d.a"), "DATASET")
        # DataHub's total can be higher than what it actually returns.
        client.query = MagicMock(
            side_effect=[
                lineage_page([a], start=0, total=10),
                lineage_page([], start=1, total=10),
            ]
        )
        assets = client.search_downstream(table_urn("p.d.t"))
        assert [x["urn"] for x in assets] == [table_urn("p.d.a")]
        assert client.query.call_count == 2

    @pytest.mark.parametrize(
        "max_degree,degrees",
        [
            (None, ["1", "2", "3+"]),
            (1, ["1"]),
            (2, ["1", "2"]),
            (3, ["1", "2", "3+"]),
        ],
    )
    def test_search_downstream_sends_degree_filter(self, client, max_degree, degrees):
        client.query = MagicMock(return_value=lineage_page([]))
        client.search_downstream(table_urn("p.d.t"), max_degree=max_degree)
        or_filters = client.query.call_args[0][1]["input"]["orFilters"]
        assert or_filters == [{"and": [{"field": "degree", "values": degrees}]}]

    def test_search_downstream_sends_types(self, client):
        client.query = MagicMock(return_value=lineage_page([]))
        client.search_downstream(table_urn("p.d.t"))
        assert "types" not in client.query.call_args[0][1]["input"]
        client.search_downstream(table_urn("p.d.t"), types={"DASHBOARD", "CHART"})
        assert client.query.call_args[0][1]["input"]["types"] == [
            "CHART",
            "DASHBOARD",
        ]

    def test_usage_reads_summary_and_last_query_day(self, client):
        day = 1759536000000  # 2025-10-04 00:00 UTC
        client.query = MagicMock(
            return_value={
                "entities": [
                    {
                        "urn": "a",
                        "statsSummary": {
                            "queryCountLast30Days": 7,
                            "uniqueUserCountLast30Days": 3,
                        },
                        "usageStats": {
                            "buckets": [
                                {
                                    "bucket": day - 86400000,
                                    "metrics": {"totalSqlQueries": 4},
                                },
                                {"bucket": day, "metrics": {"totalSqlQueries": 3}},
                                {
                                    "bucket": day + 86400000,
                                    "metrics": {"totalSqlQueries": 0},
                                },
                            ]
                        },
                    },
                    {"urn": "b", "statsSummary": None, "usageStats": None},
                ]
            }
        )
        assert client.usage(["a", "b"]) == {
            "a": {"queries_30d": 7, "users_30d": 3, "last_query": "2025-10-04"},
            "b": {"queries_30d": None, "users_30d": None, "last_query": None},
        }

    def test_usage_is_batched(self, client):
        client.query = MagicMock(return_value={"entities": []})
        client.usage([f"u{i}" for i in range(250)])
        assert [len(c[0][1]["urns"]) for c in client.query.call_args_list] == [
            100,
            100,
            50,
        ]

    def test_search_downstream_sends_time_window(self, client):
        client.query = MagicMock(return_value=lineage_page([]))
        client.search_downstream(table_urn("p.d.t"), since_ms=1000)
        flags = client.query.call_args[0][1]["input"]["lineageFlags"]
        assert flags["startTimeMillis"] == 1000
        assert flags["endTimeMillis"] >= int(time.time() * 1000) - 60000


class TestGetDownstreamLineage:
    def make_client(self, assets, usage=None):
        client = MagicMock(spec=DataHubClient)
        client.search_downstream.side_effect = lambda *args, **kwargs: [
            dict(a) for a in assets
        ]
        client.usage.return_value = usage or {}
        return client

    def assets(self, *results):
        return [to_asset(r) for r in results]

    def test_drops_deleted_assets_unless_asked(self):
        live = result(table_urn("p.d.live"), "DATASET")
        gone = result(table_urn("p.d.gone"), "DATASET", removed=True)
        client = self.make_client(self.assets(live, gone))

        assets = get_downstream_lineage(client, "p.d.t")
        assert [a["urn"] for a in assets] == [table_urn("p.d.live")]

        assets = get_downstream_lineage(client, "p.d.t", include_deleted=True)
        assert len(assets) == 2

    def test_drops_old_looker_tmp_copies(self):
        old = result(table_urn("mozdata.looker_tmp.LR_4M91H1700000000000_x"), "DATASET")
        client = self.make_client(self.assets(old))
        assets = get_downstream_lineage(client, "p.d.t", looker_tmp_days=3)
        assert assets == []
        assets = get_downstream_lineage(client, "p.d.t", looker_tmp_days=0)
        assert len(assets) == 1

    def test_adds_usage_to_bigquery_datasets_only(self):
        table = result(table_urn("p.d.a"), "DATASET")
        chart = result("urn:li:chart:(redash,1)", "CHART", platform="redash")
        stats = {"queries_30d": 5, "users_30d": 2, "last_query": "2026-10-04"}
        client = self.make_client(
            self.assets(table, chart), usage={table_urn("p.d.a"): stats}
        )
        assets = get_downstream_lineage(client, "p.d.t", include_usage=True)
        client.usage.assert_called_once_with([table_urn("p.d.a")])
        assert {k: assets[0][k] for k in stats} == stats
        assert assets[1]["queries_30d"] is None

    def test_types_are_sent_to_datahub(self):
        chart = result("urn:li:chart:(redash,1)", "CHART", platform="redash")
        client = self.make_client(self.assets(chart))
        get_downstream_lineage(client, "p.d.t", include_usage=True, types={"CHART"})
        assert client.search_downstream.call_args[1]["types"] == {"CHART"}
        client.usage.assert_not_called()

    def test_usage_is_off_by_default(self):
        client = self.make_client(self.assets(result(table_urn("p.d.a"), "DATASET")))
        assets = get_downstream_lineage(client, "p.d.t")
        client.usage.assert_not_called()
        assert assets[0]["queries_30d"] is None

    def test_since_days_becomes_a_time_window(self):
        client = self.make_client([])
        get_downstream_lineage(client, "p.d.t", since_days=90)
        since_ms = client.search_downstream.call_args[0][2]
        expected = (time.time() - 90 * 86400) * 1000
        assert abs(since_ms - expected) < 60000


def table_rows(text):
    """Return the split header and rows of the first table in format_text output."""
    lines = click.unstyle(text).splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("DEGREE"))
    rows = []
    for line in lines[start + 2 :]:
        if not line:
            break
        rows.append(line.split())
    return lines[start].split(), rows


class TestFormatText:
    def test_one_section_per_platform_without_urns(self):
        assets = [
            to_asset(
                result(
                    table_urn("p.d.a"),
                    "DATASET",
                    properties={"name": "a", "qualifiedName": "p.d.a"},
                    subTypes={"typeNames": ["Table"]},
                )
            ),
            to_asset(
                result(
                    "urn:li:chart:(redash,1)",
                    "CHART",
                    degree=2,
                    platform="redash",
                    properties={"name": "Retention Table"},
                )
            ),
        ]
        text = click.unstyle(format_text("p.d.t", assets))
        lines = text.splitlines()
        assert lines[:2] == ["Downstream lineage of p.d.t", "2 assets"]
        assert "BIGQUERY (1)" in lines
        assert "REDASH (1)" in lines
        assert any(line.split()[:3] == ["1", "Table", "p.d.a"] for line in lines)
        assert any(
            line.split()[:4] == ["2", "Chart", "Retention", "Table"] for line in lines
        )
        assert "urn:li:" not in text

    def asset(self, queries_30d=None, users_30d=None, last_query=None):
        asset = to_asset(
            result(
                table_urn("p.d.a"),
                "DATASET",
                properties={"name": "a", "qualifiedName": "p.d.a"},
                subTypes={"typeNames": ["Table"]},
            )
        )
        asset.update(
            queries_30d=queries_30d, users_30d=users_30d, last_query=last_query
        )
        return asset

    def test_bigquery_rows_show_usage(self):
        asset = self.asset(queries_30d=5, users_30d=2, last_query="2026-10-04")
        header, rows = table_rows(format_text("p.d.t", [asset], show_usage=True))
        assert header == [
            "DEGREE",
            "TYPE",
            "NAME",
            "QUERIES",
            "30D",
            "USERS",
            "30D",
            "LAST",
            "QUERY",
        ]
        assert rows == [["1", "Table", "p.d.a", "5", "2", "2026-10-04"]]

    @pytest.mark.parametrize(
        "queries_30d,users_30d,last_query,shown",
        [
            (None, None, None, ["n/a", "n/a", "n/a"]),
            (0, 0, None, ["0", "0", "none"]),
            (24, 2, None, ["24", "2", "n/a"]),
        ],
    )
    def test_usage_cells(self, queries_30d, users_30d, last_query, shown):
        asset = self.asset(queries_30d, users_30d, last_query)
        _, rows = table_rows(format_text("p.d.t", [asset], show_usage=True))
        assert rows[0][3:6] == shown

    def test_no_usage_columns_by_default(self):
        asset = self.asset(queries_30d=5, users_30d=2, last_query="2026-10-04")
        header, _ = table_rows(format_text("p.d.t", [asset]))
        assert header == ["DEGREE", "TYPE", "NAME"]
