import json

import pytest

from bigquery_etl.data_governance.classification import taxonomy as taxonomy_module
from bigquery_etl.data_governance.classification.taxonomy import (
    load_taxonomy,
    taxonomy_prompt_block,
    taxonomy_version,
)


def write_taxonomy(tmp_path, entries):
    """Write entries to a taxonomy_v1.json in tmp_path and return the path."""
    path = tmp_path / "taxonomy_v1.json"
    path.write_text(json.dumps(entries))
    return path


class TestLoadTaxonomy:
    def test_committed_taxonomy(self):
        taxonomy = load_taxonomy()

        labels = {entry["label"] for entry in taxonomy}
        assert {
            "system",
            "user.unique_id",
            "user.unique_id.client_id",
            "user.account.fxid",
            "user.behavior.search.term",
        } <= labels
        required_fields = {
            "label",
            "parent",
            "level",
            "display_name",
            "description",
            "examples",
        }
        for entry in taxonomy:
            assert required_fields <= set(entry)
            assert entry["level"] in {"subject", "data_type", "subcategory"}
            assert isinstance(entry["label"], str)
            assert all(
                entry[field] is None or isinstance(entry[field], str)
                for field in {"display_name", "description", "examples"}
            )
            parent = entry["parent"]
            assert parent is None or parent in labels
            if parent is not None:
                assert entry["label"].startswith(f"{parent}.")

    @pytest.mark.parametrize(
        "entries",
        [
            [],
            [{"label": "user"}, {"display_name": "System"}],
            [{"label": "user"}, {"label": "user"}],
        ],
        ids=["empty", "missing-label", "duplicate-label"],
    )
    def test_invalid_taxonomy_rejected(self, tmp_path, monkeypatch, entries):
        monkeypatch.setattr(
            taxonomy_module, "TAXONOMY_PATH", write_taxonomy(tmp_path, entries)
        )

        with pytest.raises(ValueError):
            load_taxonomy()


class TestTaxonomyVersion:
    def test_version_from_filename(self):
        assert taxonomy_version() == "v1"

    def test_unversioned_filename_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            taxonomy_module, "TAXONOMY_PATH", tmp_path / "taxonomy.json"
        )

        with pytest.raises(ValueError):
            taxonomy_version()


class TestTaxonomyPromptBlock:
    def test_key_order_renaming_and_compact_separators(self):
        block = taxonomy_prompt_block(
            [
                {
                    "label": "user",
                    "parent": None,
                    "level": "subject",
                    "display_name": "User",
                    "description": None,
                    "examples": "a client id",
                }
            ]
        )

        assert block == (
            '[{"label":"user","name":"User","desc":"","examples":"a client id"}]'
        )
