"""Tests for the sensitive-data-flow check (bigquery_etl/sensitivity.py)."""

from pathlib import Path

from click.testing import CliRunner

# Import the bqetl CLI before the data_governance group: the group takes an
# option from bigquery_etl.cli.utils and bigquery_etl.cli imports the group
# back, so importing the group first finds it partially initialized.
from bigquery_etl.cli import cli as _bqetl_cli  # noqa: F401
from bigquery_etl.data_governance.cli import sensitivity as sensitivity_cmd
from bigquery_etl.sensitivity import (
    check_query,
    dataset_access,
    effective_access,
    gated_readers,
    load_codeowners,
    path_owners,
    source_access,
)

PROJECT = "moz-fx-data-shared-prod"
BROAD = "workgroup:mozilla-confidential/data-viewers"


def _dataset(sql_dir, name, base_acl="derived", members=(BROAD,), project=PROJECT):
    ds = sql_dir / project / name
    ds.mkdir(parents=True, exist_ok=True)
    wa = "workgroup_access:\n- role: roles/bigquery.dataViewer\n  members:\n" + "".join(
        f"  - {m}\n" for m in members
    )
    (ds / "dataset_metadata.yaml").write_text(
        f"friendly_name: {name}\ndescription: d\n"
        f"dataset_base_acl: {base_acl}\nuser_facing: false\n{wa}"
    )
    return ds


def _query(ds, table, sql, table_members=None):
    t = ds / table
    t.mkdir(parents=True, exist_ok=True)
    (t / "query.sql").write_text(sql)
    if table_members is not None:
        wa = (
            "workgroup_access:\n- role: roles/bigquery.dataViewer\n  members:\n"
            + "".join(f"  - {m}\n" for m in table_members)
        )
        (t / "metadata.yaml").write_text(
            f"friendly_name: {table}\ndescription: d\nowners:\n  - a@b.org\n{wa}"
        )
    return str(t / "query.sql")


def _select(dataset, table="t", project=PROJECT):
    return f"SELECT * FROM `{project}.{dataset}.{table}`"


class TestAccessResolution:
    def test_sensitivity_classification(self, tmp_path):
        sql = tmp_path / "sql"
        _dataset(sql, "restricted_ds", base_acl="restricted")
        _dataset(sql, "broad_ds", base_acl="derived", members=[BROAD])
        _dataset(sql, "narrow_ds", base_acl="derived", members=["workgroup:acme/x"])

        assert dataset_access(str(sql), PROJECT, "restricted_ds").sensitive is True
        # broad, non-restricted -> not sensitive
        assert dataset_access(str(sql), PROJECT, "broad_ds").sensitive is False
        # scoped to a narrow workgroup -> sensitive
        assert dataset_access(str(sql), PROJECT, "narrow_ds").sensitive is True

    def test_unresolved_dataset_returns_none(self, tmp_path):
        sql = tmp_path / "sql"
        assert dataset_access(str(sql), PROJECT, "does_not_exist") is None

    def test_effective_access_unions_table_readers(self, tmp_path):
        sql = tmp_path / "sql"
        ds = _dataset(
            sql, "foo_external", base_acl="restricted", members=["workgroup:a/x"]
        )
        _query(ds, "tbl", "SELECT 1", table_members=["workgroup:a/x", "workgroup:b/y"])
        acc = effective_access(str(sql), PROJECT, "foo_external", "tbl")
        # table-level grant (b/y) is unioned on top of the dataset grant (a/x)
        assert acc.readers == {"workgroup:a/x", "workgroup:b/y"}


class TestFlowCheck:
    def test_flags_restricted_source_to_broad_dest(self, tmp_path):
        sql = tmp_path / "sql"
        _dataset(
            sql, "src_external", base_acl="restricted", members=["workgroup:acme/x"]
        )
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("src_external"))

        findings = check_query(q, str(sql))
        assert len(findings) == 1
        assert findings[0]["source"] == f"{PROJECT}.src_external"
        assert findings[0]["extra_readers"] == [BROAD]

    def test_no_flag_when_dest_same_workgroup(self, tmp_path):
        sql = tmp_path / "sql"
        _dataset(
            sql, "src_external", base_acl="restricted", members=["workgroup:acme/x"]
        )
        dest = _dataset(
            sql, "acme_derived", base_acl="restricted", members=["workgroup:acme/x"]
        )
        q = _query(dest, "out_v1", _select("src_external"))

        assert check_query(q, str(sql)) == []

    def test_no_flag_when_source_table_grants_dest_workgroup(self, tmp_path):
        sql = tmp_path / "sql"
        # dataset is narrow, but the source *table* widens to the dest's workgroup
        src = _dataset(
            sql, "src_external", base_acl="restricted", members=["workgroup:acme/x"]
        )
        _query(
            src,
            "wide_tbl",
            "SELECT 1",
            table_members=["workgroup:acme/x", "workgroup:team/y"],
        )
        dest = _dataset(
            sql, "team_derived", base_acl="restricted", members=["workgroup:team/y"]
        )
        q = _query(dest, "out_v1", _select("src_external", table="wide_tbl"))

        assert check_query(q, str(sql)) == []

    def test_mozfun_source_is_safe(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("stats", table="foo", project="mozfun"))

        assert check_query(q, str(sql)) == []

    def test_unresolved_source_treated_as_sensitive(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        # an unknown other-project dataset can't be resolved -> unsafe
        q = _query(
            dest,
            "out_v1",
            _select("other_dataset", table="t", project="other-project"),
        )

        findings = check_query(q, str(sql), gated_datasets={})
        assert len(findings) == 1
        assert "unresolved" in findings[0]["source_base_acl"]
        assert findings[0]["extra_readers"] == [BROAD]

    def test_information_schema_ignored(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(
            dest,
            "out_v1",
            "SELECT * FROM `region-us`.INFORMATION_SCHEMA.JOBS",
        )

        assert check_query(q, str(sql)) == []


class TestCodeowners:
    def _codeowners(self, tmp_path):
        (tmp_path / "CODEOWNERS").write_text(
            "* @mozilla/dataplatform-wg\n"
            "/sql/\n"  # explicitly unowned
            f"/sql/{PROJECT}/owned_derived/** @mozilla/team\n"
        )
        return load_codeowners(str(tmp_path / "CODEOWNERS"))

    def test_path_owners_last_match_wins(self, tmp_path):
        entries = self._codeowners(tmp_path)
        assert path_owners(f"sql/{PROJECT}/plain_derived/x/query.sql", entries) == []
        assert path_owners(f"sql/{PROJECT}/owned_derived/x/query.sql", entries) == [
            "@mozilla/team"
        ]

    def test_finding_gated_flag_reflects_ownership(self, tmp_path, monkeypatch):
        # CODEOWNERS patterns are repo-relative, so run from the repo root and
        # use a relative sql dir/query path
        monkeypatch.chdir(tmp_path)
        sql = Path("sql")
        _dataset(
            sql, "src_external", base_acl="restricted", members=["workgroup:acme/x"]
        )
        dest = _dataset(sql, "owned_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("src_external"))

        entries = self._codeowners(tmp_path)
        findings = check_query(q, "sql", codeowners=entries)
        assert len(findings) == 1
        assert findings[0]["gated"] is True
        assert findings[0]["owners"] == ["@mozilla/team"]


# A minimal gated_datasets config mirroring the bqetl_project.yaml shape:
# keyed by project, then dataset -- a narrow dataset, a broad dataset with
# per-doctype gated tables, and a second project.
GATED = {
    PROJECT: {
        "acme_stable": {"default": ["workgroup:acme/data-viewers"]},
        "telemetry_stable": {
            "default": [BROAD],
            "tables": {
                "account_ecosystem": [],
                "serp_categorization": ["workgroup:revenue/cat2"],
            },
        },
    },
    "other-project": {
        "glam_stable": {"default": ["workgroup:glam/data-viewers"]},
    },
}


class TestGatedDatasets:
    def test_table_override_beats_default_and_strips_version(self):
        # version suffix (_v5) stripped before matching the doctype key
        assert gated_readers(
            GATED, PROJECT, "telemetry_stable", "serp_categorization_v5"
        ) == {"workgroup:revenue/cat2"}

    def test_falls_back_to_default_when_table_unlisted(self):
        assert gated_readers(GATED, PROJECT, "telemetry_stable", "main_v5") == {BROAD}

    def test_empty_members_is_restricted(self):
        assert (
            gated_readers(GATED, PROJECT, "telemetry_stable", "account_ecosystem_v1")
            == set()
        )

    def test_unlisted_dataset_returns_none(self):
        assert gated_readers(GATED, PROJECT, "not_listed_stable", "t_v1") is None

    def test_resolves_per_project(self):
        assert gated_readers(GATED, "other-project", "glam_stable", "t_v1") == {
            "workgroup:glam/data-viewers"
        }

    def test_dataset_not_matched_across_projects(self):
        # acme_stable is listed under PROJECT, not other-project
        assert gated_readers(GATED, "other-project", "acme_stable", "t_v1") is None
        assert gated_readers(GATED, "unlisted-project", "acme_stable", "t_v1") is None

    def test_source_access_uses_gated_config(self, tmp_path):
        acc = source_access(
            str(tmp_path / "sql"), PROJECT, "acme_stable", "t_v1", GATED
        )
        assert acc.readers == {"workgroup:acme/data-viewers"}
        assert acc.sensitive is True

    def test_source_access_unlisted_ingestion_is_broad(self, tmp_path):
        # a stable/live dataset absent from a listed project gets the broad default
        acc = source_access(
            str(tmp_path / "sql"), PROJECT, "unlisted_stable", "t_v1", GATED
        )
        assert acc.readers == {BROAD}
        assert acc.sensitive is False

    def test_source_access_unlisted_project_unresolved(self, tmp_path):
        # a stable dataset in a project not keyed in gated_datasets -> unresolved
        acc = source_access(
            str(tmp_path / "sql"), "unlisted-project", "some_stable", "t_v1", GATED
        )
        assert acc is None

    def test_source_access_second_ingestion_project_is_broad(self, tmp_path):
        # other-project is keyed in gated_datasets, so its unlisted stable dataset
        # is broad rather than unresolved
        acc = source_access(
            str(tmp_path / "sql"), "other-project", "some_stable", "t_v1", GATED
        )
        assert acc.readers == {BROAD}
        assert acc.sensitive is False

    def test_flags_gated_dataset_source(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("acme_stable", table="t_v1"))

        findings = check_query(q, str(sql), gated_datasets=GATED)
        assert len(findings) == 1
        assert findings[0]["source"] == f"{PROJECT}.acme_stable"
        assert findings[0]["extra_readers"] == [BROAD]

    def test_flags_gated_table_in_broad_dataset(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(
            dest, "out_v1", _select("telemetry_stable", table="serp_categorization_v1")
        )

        findings = check_query(q, str(sql), gated_datasets=GATED)
        assert len(findings) == 1
        assert findings[0]["extra_readers"] == [BROAD]

    def test_no_flag_for_broad_table_in_broad_dataset(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        # telemetry_stable.main is broad (default), dest is broad -> no widening
        q = _query(dest, "out_v1", _select("telemetry_stable", table="main_v5"))

        assert check_query(q, str(sql), gated_datasets=GATED) == []

    def test_no_flag_for_unlisted_ingestion_source(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("unlisted_stable", table="t_v1"))

        assert check_query(q, str(sql), gated_datasets=GATED) == []


class TestSensitivityCommand:
    """`./bqetl data_governance sensitivity` CLI wrapper."""

    def _widening_query(self, tmp_path):
        sql = tmp_path / "sql"
        _dataset(
            sql, "src_external", base_acl="restricted", members=["workgroup:acme/x"]
        )
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", _select("src_external"))
        return sql, q

    def _codeowners(self, tmp_path, body):
        p = tmp_path / "CODEOWNERS"
        p.write_text(body)
        return str(p)

    def test_ungated_flow_exits_2_and_requests_review(self, tmp_path):
        sql, q = self._widening_query(tmp_path)
        # CODEOWNERS that owns nothing here -> the flow is ungated
        codeowners = self._codeowners(tmp_path, "/sql/owned/** @mozilla/team\n")

        result = CliRunner().invoke(
            sensitivity_cmd,
            [q, "--sql_dir", str(sql), "--codeowners", codeowners],
        )
        assert result.exit_code == 2
        assert f"{PROJECT}.src_external" in result.output
        assert "needs Data Platform" in result.output
        assert "::warning::" in result.stderr

    def test_gated_flow_exits_0(self, tmp_path):
        sql, q = self._widening_query(tmp_path)
        # CODEOWNERS owns everything -> the flow is already reviewed, not ungated
        codeowners = self._codeowners(tmp_path, "* @mozilla/team\n")

        result = CliRunner().invoke(
            sensitivity_cmd,
            [q, "--sql_dir", str(sql), "--codeowners", codeowners],
        )
        assert result.exit_code == 0
        assert "already reviewed by @mozilla/team" in result.output

    def test_clean_query_exits_0(self, tmp_path):
        sql = tmp_path / "sql"
        dest = _dataset(sql, "broad_derived", members=[BROAD])
        q = _query(dest, "out_v1", "SELECT 1")
        codeowners = self._codeowners(tmp_path, "* @mozilla/team\n")

        result = CliRunner().invoke(
            sensitivity_cmd,
            [q, "--sql_dir", str(sql), "--codeowners", codeowners],
        )
        assert result.exit_code == 0
        assert "no ungated sensitive-data flows" in result.stderr

    def test_json_output(self, tmp_path):
        import json

        sql, q = self._widening_query(tmp_path)
        codeowners = self._codeowners(tmp_path, "* @mozilla/team\n")

        result = CliRunner().invoke(
            sensitivity_cmd,
            [q, "--sql_dir", str(sql), "--codeowners", codeowners, "--json"],
        )
        assert result.exit_code == 0
        # the JSON array is on stdout; a stderr status line may follow it
        payload = result.output[: result.output.rindex("]") + 1]
        findings = json.loads(payload)
        assert findings[0]["source"] == f"{PROJECT}.src_external"
