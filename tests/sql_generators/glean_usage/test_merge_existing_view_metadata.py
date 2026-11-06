"""Tests for merging partial checked-in view metadata.yaml into generated metadata."""

from textwrap import dedent

import yaml

from bigquery_etl.util.common import render
from sql_generators.glean_usage.common import PATH, merge_existing_view_metadata

HEADER = "---\n# Generated via bigquery_etl.glean_usage\n"


def generated_metadata():
    return render(
        "events_stream.metadata.yaml",
        template_folder=PATH / "templates",
        format=False,
        header_yaml=HEADER,
    )


def test_no_existing_metadata(tmp_path):
    generated = generated_metadata()
    assert merge_existing_view_metadata(tmp_path / "metadata.yaml", generated) == (
        generated
    )


def test_partial_metadata_is_merged(tmp_path):
    metadata_file = tmp_path / "metadata.yaml"
    metadata_file.write_text(dedent("""\
            labels:
              authorized: true
            workgroup_access:
            - role: roles/bigquery.dataViewer
              members:
              - workgroup:example/viewers
            """))
    generated = generated_metadata()

    merged = merge_existing_view_metadata(metadata_file, generated)

    assert merged.startswith(HEADER)
    merged_metadata = yaml.safe_load(merged)
    generated_metadata_dict = yaml.safe_load(generated)
    assert merged_metadata["friendly_name"] == generated_metadata_dict["friendly_name"]
    assert merged_metadata["owners"] == generated_metadata_dict["owners"]
    assert merged_metadata["labels"] == {
        "sql_generator": "glean_usage",
        "authorized": True,
    }
    assert merged_metadata["workgroup_access"] == [
        {
            "role": "roles/bigquery.dataViewer",
            "members": ["workgroup:example/viewers"],
        }
    ]


def test_full_metadata_is_overwritten(tmp_path):
    metadata_file = tmp_path / "metadata.yaml"
    metadata_file.write_text("friendly_name: Old\ndescription: Old\n")
    generated = generated_metadata()

    assert merge_existing_view_metadata(metadata_file, generated) == generated
