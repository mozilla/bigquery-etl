from pathlib import Path
from textwrap import dedent

import yaml
from google.cloud.bigquery import SchemaField

from bigquery_etl.format_sql.formatter import reformat
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

    def test_bigquery_conversion(self):
        bq_schema = [
            SchemaField(
                "record_field",
                "RECORD",
                "REPEATED",
                description="Record field",
                fields=(
                    SchemaField(
                        "nested_field_1",
                        "STRING",
                        "NULLABLE",
                        description="Nested Field 1",
                    ),
                    SchemaField(
                        "nested_field_2",
                        "INTEGER",
                        "NULLABLE",
                        description="Nested Field 2",
                    ),
                ),
            ),
            SchemaField("normal_field", "STRING"),
        ]

        schema = Schema.from_bigquery_schema(bq_schema)
        assert schema.schema["fields"][0]["description"] == "Record field"
        assert schema.schema["fields"][0]["name"] == "record_field"
        assert (
            schema.schema["fields"][0]["fields"][0]["description"] == "Nested Field 1"
        )
        assert schema.schema["fields"][0]["fields"][0]["type"] == "STRING"
        assert schema.schema["fields"][1]["name"] == "normal_field"

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
                description: attribution campaign
              mode: NULLABLE
              name: attribution
              type: RECORD
            """
        )

        schema = Schema.from_json(yaml.safe_load(schema_yaml))
        bq_schema = schema.to_bigquery_schema()
        assert len(bq_schema) == 3
        assert bq_schema[0] == SchemaField(
            name="submission_date", field_type="DATE", mode="NULLABLE"
        )
        assert bq_schema[2] == SchemaField(
            name="attribution",
            field_type="RECORD",
            mode="NULLABLE",
            fields=(
                SchemaField(
                    name="campaign",
                    field_type="STRING",
                    description="attribution campaign",
                ),
            ),
        )


def test_generate_compatible_select_expression():
    source_schema = {
        "fields": [
            {"name": "scalar", "type": "INTEGER"},
            {"name": "mismatch_scalar", "type": "INTEGER"},
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_extra", "type": "DATE"},
                    {
                        "name": "nested_record",
                        "type": "RECORD",
                        "fields": [
                            {"name": "v1", "type": "INTEGER"},
                            {"name": "v2", "type": "INTEGER"},
                        ],
                    },
                    {
                        "name": "mismatch_record",
                        "type": "RECORD",
                        "fields": [{"name": "nested_str", "type": "STRING"}],
                    },
                ],
            },
            {"name": "array_scalar", "type": "INTEGER", "mode": "REPEATED"},
            {
                "name": "array_record",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "key", "type": "STRING"},
                    {"name": "value", "type": "STRING"},
                ],
            },
            {"name": "extra", "type": "STRING"},
        ]
    }
    target_schema = {
        "fields": [
            {"name": "scalar", "type": "INTEGER"},
            {"name": "mismatch_scalar", "type": "STRING"},
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_missing", "type": "DATE"},
                    {
                        "name": "nested_record",
                        "type": "RECORD",
                        "fields": [
                            {"name": "v1", "type": "INTEGER"},
                            {"name": "v2", "type": "INTEGER"},
                        ],
                    },
                    {
                        "name": "mismatch_record",
                        "type": "RECORD",
                        "fields": [{"name": "nested_int", "type": "INTEGER"}],
                    },
                ],
            },
            {"name": "array_scalar", "type": "INTEGER", "mode": "REPEATED"},
            {
                "name": "array_record",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "value", "type": "STRING"},
                    {"name": "key", "type": "STRING"},
                ],
            },
            {"name": "missing", "type": "STRING"},
        ]
    }
    expected_expr = """
    scalar,
    CAST(NULL AS STRING) AS `mismatch_scalar`,
    STRUCT(
        CAST(NULL AS DATE) AS `nested_missing`,
        record.nested_record,
        STRUCT(CAST(NULL AS INTEGER) AS `nested_int`) AS `mismatch_record`
    ) AS `record`,
    array_scalar,
    ARRAY(
        SELECT
            STRUCT(
                array_record.value,
                array_record.key
            )
        FROM
            UNNEST(array_record) AS `array_record`
    ) AS `array_record`,
    CAST(NULL AS STRING) AS `missing`
    """

    source = Schema.from_json(source_schema)
    target = Schema.from_json(target_schema)

    select_expr = source.generate_compatible_select_expression(
        target, unnest_structs=False
    )
    assert reformat(select_expr) == reformat(expected_expr)


def test_generate_select_expression_unnest_struct():
    """unnest_struct argument should unnest records even when they match."""
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "key", "type": "STRING"},
                    {"name": "value", "type": "STRING"},
                ],
            },
        ]
    }

    source = Schema.from_json(source_schema)

    unnest_expr = source.generate_select_expression(unnest_structs=True)
    assert reformat(unnest_expr) == reformat(
        "STRUCT(record.key, record.value) AS `record`"
    )

    no_unnest_expr = source.generate_select_expression(unnest_structs=False)
    assert reformat(no_unnest_expr) == reformat("record")


def test_generate_select_expression_remove_fields():
    """remove_fields argument remove the given fields from the output."""
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "key", "type": "STRING"},
                    {"name": "value", "type": "STRING"},
                ],
            },
            {"name": "scalar", "type": "INTEGER"},
        ]
    }

    source = Schema.from_json(source_schema)

    unnest_expr = source.generate_select_expression(
        unnest_structs=True, remove_fields=["record.value", "scalar"]
    )
    assert reformat(unnest_expr) == reformat("STRUCT(record.key) AS `record`")


def test_generate_select_expression_max_unnest_depth():
    """max_unnest_depth argument should stop unnesting at the given depth."""
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {
                        "name": "record2",
                        "type": "RECORD",
                        "fields": [
                            {
                                "name": "record3",
                                "type": "RECORD",
                                "fields": [{"name": "key", "type": "STRING"}],
                            },
                        ],
                    },
                ],
            },
        ]
    }

    source = Schema.from_json(source_schema)

    expected_expr = """
    STRUCT(
        STRUCT(
            record.record2.record3
        ) AS `record2`
    ) AS `record`
    """

    unnest_expr = source.generate_select_expression(
        unnest_structs=True, max_unnest_depth=2
    )
    assert reformat(unnest_expr) == reformat(expected_expr)


def test_generate_select_expression_unnest_allowlist():
    """unnest_allowlist argument should cause only the given fields to be unnested."""
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [{"name": "key", "type": "STRING"}],
            },
            {
                "name": "record2",
                "type": "RECORD",
                "fields": [
                    {
                        "name": "record3",
                        "type": "RECORD",
                        "fields": [{"name": "key", "type": "STRING"}],
                    }
                ],
            },
        ]
    }

    source = Schema.from_json(source_schema)

    expected_expr = """
        record,
        STRUCT(
            STRUCT(
                record2.record3.key
            ) AS `record3`
        ) AS `record2`
        """

    unnest_expr = source.generate_select_expression(
        unnest_structs=True, unnest_allowlist=["record2"]
    )
    assert reformat(unnest_expr) == reformat(expected_expr)
