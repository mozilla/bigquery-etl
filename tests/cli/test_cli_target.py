from datetime import timedelta

import click
import pytest
import yaml

from bigquery_etl.cli.target import _parse_duration, _persist_share
from bigquery_etl.util.target import MANIFEST_FILENAME


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
