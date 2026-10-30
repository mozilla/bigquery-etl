import pytest

from bigquery_etl.util.sql_overlay import (
    ChangedOverrideError,
    changed_overrides,
    overlay_sql_dir,
)

GENERATED_METADATA = """friendly_name: App-specific view for Glean ping "crash"
description: |-
  This a view that UNIONs the stable ping tables.
owners: []
labels:
  sql_generator: glean_usage
workgroup_access:
- role: roles/bigquery.dataViewer
  members:
  - workgroup:crash-ping-ingest
  - workgroup:mozilla-confidential/data-viewers
"""

PARTIAL_METADATA = """workgroup_access:
- role: roles/bigquery.dataViewer
  members:
  # comment
  - workgroup:crash-ping-ingest
"""

CRASH_METADATA = "p/fenix/crash/metadata.yaml"


def _write(root, rel, content):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


@pytest.fixture
def dirs(tmp_path):
    src, dest, base = tmp_path / "src", tmp_path / "dest", tmp_path / "base"
    for d in (src, dest, base):
        d.mkdir()
    return src, dest, base


class TestOverlaySqlDir:
    def test_copies_new_and_changed_files(self, dirs):
        src, dest, base = dirs
        _write(src, "p/d/new_v1/query.sql", "SELECT 2")
        _write(src, "p/d/existing_v1/query.sql", "SELECT 3")
        _write(dest, "p/d/existing_v1/query.sql", "SELECT 1")
        _write(dest, "p/d/generated/view.sql", "SELECT 4")

        overlay_sql_dir(src, dest, base)

        assert (dest / "p/d/new_v1/query.sql").read_text() == "SELECT 2"
        assert (dest / "p/d/existing_v1/query.sql").read_text() == "SELECT 3"
        assert (dest / "p/d/generated/view.sql").read_text() == "SELECT 4"

    def test_keeps_generated_metadata_when_override_unchanged(self, dirs):
        src, dest, base = dirs
        _write(src, CRASH_METADATA, PARTIAL_METADATA)
        _write(base, CRASH_METADATA, PARTIAL_METADATA)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        overlay_sql_dir(src, dest, base)

        assert (dest / CRASH_METADATA).read_text() == GENERATED_METADATA

    def test_raises_when_override_changed(self, dirs):
        src, dest, base = dirs
        _write(src, CRASH_METADATA, PARTIAL_METADATA + "  - workgroup:new\n")
        _write(base, CRASH_METADATA, PARTIAL_METADATA)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        with pytest.raises(ChangedOverrideError):
            overlay_sql_dir(src, dest, base)

    def test_full_metadata_replaces_generated_metadata(self, dirs):
        src, dest, base = dirs
        full = "friendly_name: Crash\ndescription: Checked in\n"
        _write(src, CRASH_METADATA, full)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        overlay_sql_dir(src, dest, base)

        assert (dest / CRASH_METADATA).read_text() == full

    def test_partial_metadata_over_non_generated_metadata_is_copied(self, dirs):
        src, dest, base = dirs
        _write(src, "p/d/t_v1/metadata.yaml", PARTIAL_METADATA)
        _write(dest, "p/d/t_v1/metadata.yaml", "friendly_name: T\nowners: []\n")

        overlay_sql_dir(src, dest, base)

        assert (dest / "p/d/t_v1/metadata.yaml").read_text() == PARTIAL_METADATA


class TestChangedOverrides:
    def test_unchanged_override(self, dirs):
        src, dest, base = dirs
        _write(src, CRASH_METADATA, PARTIAL_METADATA)
        _write(base, CRASH_METADATA, PARTIAL_METADATA)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        assert changed_overrides(src, dest, base) == []

    def test_changed_override(self, dirs):
        src, dest, base = dirs
        _write(src, CRASH_METADATA, PARTIAL_METADATA + "  - workgroup:new\n")
        _write(base, CRASH_METADATA, PARTIAL_METADATA)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        assert changed_overrides(src, dest, base) == [src / CRASH_METADATA]

    def test_new_override(self, dirs):
        src, dest, base = dirs
        _write(src, CRASH_METADATA, PARTIAL_METADATA)
        _write(dest, CRASH_METADATA, GENERATED_METADATA)

        assert changed_overrides(src, dest, base) == [src / CRASH_METADATA]

    def test_ignores_non_generated_metadata(self, dirs):
        src, dest, base = dirs
        _write(src, "p/d/t_v1/metadata.yaml", PARTIAL_METADATA)
        _write(dest, "p/d/t_v1/metadata.yaml", "friendly_name: T\nowners: []\n")

        assert changed_overrides(src, dest, base) == []
