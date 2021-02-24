from pathlib import Path
from textwrap import dedent

import yaml

from bigquery_etl.schema import Schema

TEST_DIR = Path(__file__).parent.parent


class TestQuerySchema:
    def test_from_schema_file(self):
        schema_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_v1"
            / "schema.yaml"
        )

        schema = Schema.from_schema_file(schema_file)
        assert len(schema.schema["fields"]) == 3

    def test_from_query_file(self):
        query_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "incremental_query_non_incremental_export_v1"
            / "query.sql"
        )

        schema = Schema.from_query_file(query_file)
        assert len(schema.schema["fields"]) == 3

    def test_from_json(self):
        json_schema = {
            "fields": [
                {"name": "amount", "type": "INTEGER"},
                {"name": "amount_captured", "type": "INTEGER"},
            ]
        }

        schema = Schema.from_json(json_schema)
        assert len(schema.schema["fields"]) == 2

    def test_equal_schemas(self):
        empty_schema = Schema.from_json({"fields": {}})
        assert empty_schema.equal(empty_schema)

        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            - fields:
              - mode: NULLABLE
                name: campaign
                type: STRING
              - mode: NULLABLE
                name: content
                type: STRING
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: client_id
              type: STRING
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - fields:
              - mode: NULLABLE
                name: content
                type: STRING
                description: "Cool content"
              - mode: NULLABLE
                name: campaign
                type: STRING
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is True
        assert schema_2.equal(schema_1) is True

    def test_schemas_unequal_attributes(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: INTEGER
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_unequal_fields(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: client_id
              type: STRING
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_unequal_nested_record(self):
        schema_1_yaml = dedent(
            """
            fields:
            - fields:
              - mode: NULLABLE
                name: multiprocess_compatible
                type: BOOLEAN
            mode: REPEATED
            name: active_addons
            type: RECORD
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - fields:
              - mode: NULLABLE
                name: multiprocess
                type: BOOLEAN
            mode: REPEATED
            name: active_addons
            type: RECORD
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_different_descriptions(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "The submission_date"
              type: DATE
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "Date of the submission"
              type: DATE
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is True
        assert schema_2.equal(schema_1) is True

    def test_schemas_compatible(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.compatible(schema_2) is True
        assert schema_2.compatible(schema_1) is False

    def test_merge_empty_schema(self):
        schema_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            - fields:
              - mode: NULLABLE
                name: campaign
                type: STRING
              - mode: NULLABLE
                name: content
                type: STRING
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema = Schema.from_json(yaml.safe_load(schema_yaml))
        empty_schema = Schema.from_json({"fields": []})
        schema.merge(empty_schema)
        assert schema.to_json() != empty_schema.to_json()
        assert (
            schema.to_json() == Schema.from_json(yaml.safe_load(schema_yaml)).to_json()
        )

        empty_schema.merge(schema)
        assert (
            empty_schema.to_json()
            == Schema.from_json(yaml.safe_load(schema_yaml)).to_json()
        )

    def test_merge_compatible_schemas(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            - fields:
              - mode: NULLABLE
                name: campaign
                type: STRING
              - mode: NULLABLE
                name: content
                type: STRING
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: sample_id
              type: INTEGER
            - fields:
              - mode: NULLABLE
                name: description
                type: STRING
              - mode: NULLABLE
                name: content
                description: "Content description"
                type: STRING
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert len(schema_1.schema["fields"]) == 3
        schema_1.merge(schema_2)
        assert len(schema_1.schema["fields"]) == 4

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        assert len(schema_2.schema["fields"]) == 3
        schema_2.merge(schema_1)
        assert len(schema_2.schema["fields"]) == 4

    def test_merge_different_descriptions(self):
        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "The submission_date"
              type: DATE
            """
        )

        schema_2_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "Date of the submission"
              type: DATE
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))
        schema_1.merge(schema_2)

        assert schema_1.schema["fields"][0]["description"] == "The submission_date"

        schema_1_yaml = dedent(
            """
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """
        )

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_1.merge(schema_2)

        assert schema_1.schema["fields"][0]["description"] == "Date of the submission"
