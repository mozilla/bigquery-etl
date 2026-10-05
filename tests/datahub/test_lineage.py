import time
from unittest.mock import MagicMock, patch

import click
import pytest

from bigquery_etl.datahub.lineage import (
    AuthError,
    DataHubClient,
    LineageTooLargeError,
    format_text,
    get_downstream_lineage,
    is_stale_looker_tmp,
    qualified_name,
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

    @patch("bigquery_etl.datahub.lineage.time.sleep")
    def test_rejected_env_token_is_not_retried(self, sleep, monkeypatch):
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "bad")
        client = DataHubClient(url="https://datahub.example")
        client.session.post = MagicMock(return_value=response(status=401))
        with pytest.raises(AuthError):
            client.query("q", {})
        assert client.session.post.call_count == 1

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
    def test_does_not_retry_a_rejected_token(self, sleep, client):
        client.session.post = MagicMock(return_value=response(status=401))
        with pytest.raises(AuthError):
            client.query("q", {})
        assert client.session.post.call_count == 1

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

    def test_keeps_charts_and_their_dashboards(self):
        chart = result("urn:li:chart:(redash,1)", "CHART", platform="redash")
        dashboard = result(
            "urn:li:dashboard:(redash,9)", "DASHBOARD", platform="redash"
        )
        client = self.make_client(self.assets(chart, dashboard))
        assets = get_downstream_lineage(client, "p.d.t")
        assert [a["urn"] for a in assets] == [
            "urn:li:chart:(redash,1)",
            "urn:li:dashboard:(redash,9)",
        ]

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

    def test_bigquery_rows_show_usage_then_url(self):
        asset = self.asset(queries_30d=5, users_30d=2, last_query="2026-10-04")
        lines = click.unstyle(
            format_text("p.d.t", [asset], show_usage=True)
        ).splitlines()
        assert lines[4].split() == [
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
        assert lines[6].split() == [
            "1",
            "Table",
            "p.d.a",
            "5",
            "2",
            "2026-10-04",
        ]

    @pytest.mark.parametrize(
        "queries_30d,users_30d,last_query,shown",
        [
            (None, None, None, ["0", "0", "n/a"]),
            (0, 0, None, ["0", "0", "none"]),
            (24, 2, None, ["24", "2", "n/a"]),
        ],
    )
    def test_usage_cells(self, queries_30d, users_30d, last_query, shown):
        asset = self.asset(queries_30d, users_30d, last_query)
        row = (
            click.unstyle(format_text("p.d.t", [asset], show_usage=True))
            .splitlines()[6]
            .split()
        )
        assert row[3:6] == shown

    def test_no_usage_columns_by_default(self):
        asset = self.asset(queries_30d=5, users_30d=2, last_query="2026-10-04")
        lines = click.unstyle(format_text("p.d.t", [asset])).splitlines()
        assert lines[4].split() == ["DEGREE", "TYPE", "NAME"]
