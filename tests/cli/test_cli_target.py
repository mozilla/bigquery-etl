from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import click
import pytest
import yaml

from bigquery_etl.cli.target import (
    _build_rename_plan,
    _make_transform,
    _narrow_to_newest_commit,
    _parse_duration,
    _persist_share,
    _rewrite_target_references,
)
from bigquery_etl.util.target import MANIFEST_FILENAME, Target


class TestParseDuration:
    def test_days(self):
        assert _parse_duration("7d") == timedelta(days=7)

    def test_hours(self):
        assert _parse_duration("24h") == timedelta(hours=24)

    def test_weeks(self):
        assert _parse_duration("2w") == timedelta(weeks=2)

    def test_minutes(self):
        assert _parse_duration("30m") == timedelta(minutes=30)

    def test_seconds(self):
        assert _parse_duration("30s") == timedelta(seconds=30)

    def test_compound(self):
        assert _parse_duration("1d12h") == timedelta(days=1, hours=12)

    def test_invalid_unit(self):
        with pytest.raises(click.BadParameter):
            _parse_duration("7y")

    def test_invalid_format(self):
        with pytest.raises(click.BadParameter):
            _parse_duration("abc")

    def test_empty_string(self):
        with pytest.raises(click.BadParameter):
            _parse_duration("")


class TestPersistShare:
    def test_writes_new_entry(self, tmp_path):
        table_dir = tmp_path / "proj" / "ds" / "tbl"
        table_dir.mkdir(parents=True)
        manifest_path = table_dir / MANIFEST_FILENAME
        manifest_path.write_text(yaml.dump({"source": "test"}))

        _persist_share(tmp_path, "proj", "ds", "tbl", "a@b.com", "READER")

        manifest = yaml.safe_load(manifest_path.read_text())
        assert manifest["shared_with"] == [{"email": "a@b.com", "role": "READER"}]
        assert manifest["source"] == "test"

    def test_deduplicates(self, tmp_path):
        table_dir = tmp_path / "proj" / "ds" / "tbl"
        table_dir.mkdir(parents=True)
        manifest_path = table_dir / MANIFEST_FILENAME
        manifest_path.write_text(
            yaml.dump({"shared_with": [{"email": "a@b.com", "role": "READER"}]})
        )

        _persist_share(tmp_path, "proj", "ds", "tbl", "a@b.com", "READER")

        manifest = yaml.safe_load(manifest_path.read_text())
        assert len(manifest["shared_with"]) == 1

    def test_allows_different_roles(self, tmp_path):
        table_dir = tmp_path / "proj" / "ds" / "tbl"
        table_dir.mkdir(parents=True)
        manifest_path = table_dir / MANIFEST_FILENAME
        manifest_path.write_text(
            yaml.dump({"shared_with": [{"email": "a@b.com", "role": "READER"}]})
        )

        _persist_share(tmp_path, "proj", "ds", "tbl", "a@b.com", "WRITER")

        manifest = yaml.safe_load(manifest_path.read_text())
        assert len(manifest["shared_with"]) == 2

    def test_no_manifest_is_noop(self, tmp_path):
        _persist_share(tmp_path, "proj", "ds", "tbl", "a@b.com", "READER")
        assert not (tmp_path / "proj" / "ds" / "tbl" / MANIFEST_FILENAME).exists()


class TestRewriteTargetReferences:
    def _setup(self, tmp_path):
        project_dir = tmp_path / "proj"
        (project_dir / "ds" / "tbl").mkdir(parents=True)
        (project_dir / "ds2" / "tbl2").mkdir(parents=True)
        return project_dir

    def test_replaces_in_sql_and_yaml(self, tmp_path):
        project_dir = self._setup(tmp_path)
        sql_file = project_dir / "ds" / "tbl" / "query.sql"
        sql_file.write_text("SELECT * FROM proj.user_oldname_ds.tbl")
        yaml_file = project_dir / "ds" / "tbl" / "metadata.yaml"
        yaml_file.write_text("owners:\n  - user_oldname_owner\n")

        _rewrite_target_references(
            str(tmp_path), "proj", _make_transform([("oldname", "newname")])
        )

        assert "user_newname_ds.tbl" in sql_file.read_text()
        assert "user_newname_owner" in yaml_file.read_text()

    def test_skips_non_sql_yaml(self, tmp_path):
        project_dir = self._setup(tmp_path)
        txt_file = project_dir / "ds" / "tbl" / "notes.txt"
        txt_file.write_text("oldname should stay")

        _rewrite_target_references(
            str(tmp_path), "proj", _make_transform([("oldname", "newname")])
        )

        assert txt_file.read_text() == "oldname should stay"

    def test_missing_project_dir_is_noop(self, tmp_path):
        _rewrite_target_references(
            str(tmp_path),
            "nonexistent",
            _make_transform([("oldname", "newname")]),
        )


def _mock_client(datasets):
    """Build a MagicMock bigquery.Client with list_tables/list_routines stubbed."""
    client = MagicMock()
    client.list_tables.side_effect = lambda path: [
        _mock_table(t, ttype) for t, ttype in datasets.get(path, {}).get("tables", [])
    ]
    client.list_routines.side_effect = lambda path: [
        _mock_routine(r) for r in datasets.get(path, {}).get("routines", [])
    ]
    return client


def _mock_table(table_id, ttype):
    t = MagicMock()
    t.table_id = table_id
    t.table_type = ttype
    return t


def _mock_routine(routine_id):
    r = MagicMock()
    r.routine_id = routine_id
    return r


class _FakeDataset:
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id


class TestBuildRenamePlan:
    def test_applies_branch_and_commit_transforms(self):
        old_ds = "dev_feat_old_abc1234_moz_fx"
        ds = _FakeDataset(old_ds)
        client = _mock_client(
            {
                f"proj.{old_ds}": {
                    "tables": [("feat_old_abc1234_tbl", "TABLE")],
                    "routines": [],
                }
            }
        )
        transform = _make_transform([("feat_old", "feat_new"), ("abc1234", "def5678")])

        plan = _build_rename_plan(client, "proj", [ds], transform, transform)

        assert len(plan) == 1
        old_ds_name, new_ds_name, renames = plan[0]
        assert old_ds_name == old_ds
        assert new_ds_name == "dev_feat_new_def5678_moz_fx"
        assert renames == [("feat_old_abc1234_tbl", "feat_new_def5678_tbl", "TABLE")]

    def test_identity_transforms_produce_empty_plan(self):
        """When nothing changes (no branch/commit match), that dataset is skipped."""
        old_ds = "dev_feat_old_abc1234_moz_fx"
        ds = _FakeDataset(old_ds)
        client = _mock_client(
            {
                f"proj.{old_ds}": {
                    "tables": [("feat_old_abc1234_tbl", "TABLE")],
                    "routines": [],
                }
            }
        )
        identity = _make_transform([])

        plan = _build_rename_plan(client, "proj", [ds], identity, identity)

        assert plan == []

    def test_dataset_only_branch_rename(self):
        """rename_ds_id renames the dataset; rename_art_id identity leaves tables alone."""
        old_ds = "dev_old_xx"
        ds = _FakeDataset(old_ds)
        client = _mock_client(
            {f"proj.{old_ds}": {"tables": [("tbl", "TABLE")], "routines": []}}
        )
        rename_ds = _make_transform([("old", "new")])
        identity = _make_transform([])

        plan = _build_rename_plan(client, "proj", [ds], rename_ds, identity)

        assert plan == [("dev_old_xx", "dev_new_xx", [("tbl", "tbl", "TABLE")])]


class TestNarrowToNewestCommit:
    def test_no_commit_template_returns_unchanged(self):
        target = Target(
            name="dev",
            project_id="proj",
            raw_dataset_prefix="dev_{{ git.branch }}_",
        )
        matching = [_FakeDataset("dev_feat_a"), _FakeDataset("dev_feat_b")]

        narrowed, commit = _narrow_to_newest_commit(
            MagicMock(), "proj", target, matching, "feat"
        )

        assert narrowed == matching
        assert commit is None

    def test_keeps_all_datasets_from_newest_commit_group(self):
        target = Target(
            name="dev",
            project_id="proj",
            raw_dataset_prefix="dev_{{ git.branch }}_{{ git.commit }}_{{ artifact.project_id }}_",
        )
        # 2 datasets for commit aaaa1234 (older), 2 for bbbb5678 (newer).
        matching = [
            _FakeDataset("dev_feat_aaaa1234_proj_one"),
            _FakeDataset("dev_feat_aaaa1234_proj_two"),
            _FakeDataset("dev_feat_bbbb5678_proj_one"),
            _FakeDataset("dev_feat_bbbb5678_proj_two"),
        ]
        times = {
            "dev_feat_aaaa1234_proj_one": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "dev_feat_aaaa1234_proj_two": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "dev_feat_bbbb5678_proj_one": datetime(2026, 1, 10, tzinfo=timezone.utc),
            "dev_feat_bbbb5678_proj_two": datetime(2026, 1, 11, tzinfo=timezone.utc),
        }
        client = MagicMock()
        client.get_dataset.side_effect = lambda path: MagicMock(
            modified=times[path.split(".")[-1]]
        )

        narrowed, commit = _narrow_to_newest_commit(
            client, "proj", target, matching, "feat"
        )

        assert commit == "bbbb5678"
        assert sorted(ds.dataset_id for ds in narrowed) == [
            "dev_feat_bbbb5678_proj_one",
            "dev_feat_bbbb5678_proj_two",
        ]
