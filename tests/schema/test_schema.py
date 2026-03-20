from pathlib import Path
from textwrap import dedent

import pytest
import yaml
from google.cloud.bigquery import SchemaField

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import Schema, SchemaLoader
from bigquery_etl.schema.stable_table_schema import SchemaFile

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

    def test_get_field(self):
        json_schema = {
            "fields": [
                {"name": "submission_timestamp", "type": "TIMESTAMP"},
                {
                    "name": "client_info",
                    "type": "RECORD",
                    "fields": [
                        {"name": "app_channel", "type": "STRING"},
                        {"name": "client_id", "type": "STRING"},
                    ],
                },
            ]
        }
        schema = Schema.from_json(json_schema)
        assert schema.get_field("submission_timestamp") == json_schema["fields"][0]
        assert (
            schema.get_field("client_info.app_channel")
            == json_schema["fields"][1]["fields"][0]
        )
        with pytest.raises(KeyError):
            schema.get_field("foo")
        with pytest.raises(KeyError):
            schema.get_field("ping_info.client_id")
        with pytest.raises(KeyError):
            schema.get_field("client_info.ping_type")

    def test_equal_schemas(self):
        empty_schema = Schema.from_json({"fields": {}})
        assert empty_schema.equal(empty_schema)

        schema_1_yaml = dedent("""
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
            """)

        schema_2_yaml = dedent("""
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
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is True
        assert schema_2.equal(schema_1) is True

    def test_schemas_unequal_attributes(self):
        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """)

        schema_2_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: INTEGER
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_unequal_fields(self):
        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            """)

        schema_2_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: client_id
              type: STRING
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_unequal_nested_record(self):
        schema_1_yaml = dedent("""
            fields:
            - fields:
              - mode: NULLABLE
                name: multiprocess_compatible
                type: BOOLEAN
            mode: REPEATED
            name: active_addons
            type: RECORD
            """)

        schema_2_yaml = dedent("""
            fields:
            - fields:
              - mode: NULLABLE
                name: multiprocess
                type: BOOLEAN
            mode: REPEATED
            name: active_addons
            type: RECORD
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is False
        assert schema_2.equal(schema_1) is False

    def test_schemas_different_descriptions(self):
        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "The submission_date"
              type: DATE
            """)

        schema_2_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "Date of the submission"
              type: DATE
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.equal(schema_2) is True
        assert schema_2.equal(schema_1) is True

    def test_schemas_compatible(self):
        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """)

        schema_2_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            - mode: NULLABLE
              name: client_id
              type: STRING
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))

        assert schema_1.compatible(schema_2) is True
        assert schema_2.compatible(schema_1) is False

    def test_merge_empty_schema(self):
        schema_yaml = dedent("""
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
            """)

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
        schema_1_yaml = dedent("""
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
            """)

        schema_2_yaml = dedent("""
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
            """)

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
        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "The submission_date"
              type: DATE
            """)

        schema_2_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              description: "Date of the submission"
              type: DATE
            """)

        schema_1 = Schema.from_json(yaml.safe_load(schema_1_yaml))
        schema_2 = Schema.from_json(yaml.safe_load(schema_2_yaml))
        schema_1.merge(schema_2)

        assert schema_1.schema["fields"][0]["description"] == "The submission_date"

        schema_1_yaml = dedent("""
            fields:
            - mode: NULLABLE
              name: submission_date
              type: DATE
            """)

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

        schema_yaml = dedent("""
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
            """)

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


def test_generate_compatible_select_expression_match_column_order_false():
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_1", "type": "DATE"},
                    {"name": "nested_2", "type": "DATE"},
                    {"name": "nested_3", "type": "DATE"},
                ],
            },
        ]
    }
    target_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_3", "type": "DATE"},
                    {"name": "nested_2", "type": "DATE"},
                    {"name": "nested_1", "type": "DATE"},
                ],
            },
        ]
    }
    expected_expr = "record"

    source = Schema.from_json(source_schema)
    target = Schema.from_json(target_schema)

    select_expr = source.generate_compatible_select_expression(
        target, unnest_structs=False, match_column_order=False
    )
    assert reformat(select_expr) == reformat(expected_expr)


def test_generate_compatible_select_expression_retain_null_structs():
    source_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_1", "type": "DATE"},
                    {"name": "nested_2", "type": "DATE"},
                ],
            },
            {
                "name": "array_record",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "value", "type": "STRING"},
                    {"name": "key", "type": "STRING"},
                ],
            },
            {"name": "string", "type": "STRING"},
        ]
    }
    target_schema = {
        "fields": [
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested_2", "type": "DATE"},
                    {"name": "nested_1", "type": "DATE"},
                ],
            },
            {
                "name": "array_record",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "value", "type": "STRING"},
                    {"name": "key", "type": "STRING"},
                ],
            },
            {"name": "string", "type": "STRING"},
        ]
    }
    expected_expr = """
    IF(
        record IS NULL,
        NULL,
        STRUCT(record.nested_2, record.nested_1)
    ) AS `record`,
    array_record,
    string
    """

    source = Schema.from_json(source_schema)
    target = Schema.from_json(target_schema)

    select_expr = source.generate_compatible_select_expression(
        target, unnest_structs=False, retain_null_structs=True
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


def test_generate_compatible_select_expression_description():
    source_schema = {
        "fields": [
            {"name": "scalar", "type": "INTEGER", "description": "abc"},
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested", "type": "DATE"},
                ],
            },
            {
                "name": "record2",
                "type": "RECORD",
                "fields": [
                    {"name": "nested", "type": "DATE"},
                ],
                "description": "abc",
            },
        ]
    }
    target_schema = {
        "fields": [
            {"name": "scalar", "type": "INTEGER"},
            {
                "name": "record",
                "type": "RECORD",
                "fields": [
                    {"name": "nested", "type": "DATE", "description": "abc"},
                ],
            },
            {
                "name": "record2",
                "type": "RECORD",
                "fields": [
                    {"name": "nested", "type": "DATE"},
                ],
            },
        ]
    }

    # RECORDs will be unnested if nested descriptions don't match
    expected_expr = """
    scalar,
    STRUCT(
        record.nested
    ) AS `record`,
    record2
    """

    source = Schema.from_json(source_schema)
    target = Schema.from_json(target_schema)

    select_expr = source.generate_compatible_select_expression(
        target, unnest_structs=False
    )
    assert reformat(select_expr) == reformat(expected_expr)


class PatchedSchemaLoader(SchemaLoader):
    sql_dir = TEST_DIR / "data" / "test_sql"


class TestSchemaLoader:
    nested_fields_schema_file = "/tests/data/test_sql/moz-fx-data-test-project/test/nested_fields_v1/schema.yaml"
    nested_fields_table = "moz-fx-data-test-project.test.nested_fields_v1"
    nested_fields_stable_table = "moz-fx-data-test-project.test_stable.nested_fields_v1"

    @pytest.fixture(scope="class")
    def nested_fields_schema(self):
        nested_fields_schema_file_path = (
            TEST_DIR.parent / self.nested_fields_schema_file.removeprefix("/")
        )
        with nested_fields_schema_file_path.open() as file_stream:
            return yaml.safe_load(file_stream)

    @pytest.fixture
    def patch_get_stable_table_schemas(self, monkeypatch, nested_fields_schema):
        def mock_get_stable_table_schemas():
            return [
                SchemaFile(
                    schema=nested_fields_schema["fields"],
                    schema_id="mock",
                    bq_dataset_family="test",
                    bq_table="nested_fields_v1",
                    document_namespace="test",
                    document_type="nested-fields",
                    document_version=1,
                )
            ]

        monkeypatch.setattr(
            "bigquery_etl.schema.get_stable_table_schemas",
            mock_get_stable_table_schemas,
        )

    # !include tag tests...
    def test_include_file(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            !include
            file: {self.nested_fields_schema_file}
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == nested_fields_schema

    def test_include_jmespath_field(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include
              file: {self.nested_fields_schema_file}
              jmespath: fields[?name == 'submission_timestamp'] | [0]
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": [nested_fields_schema["fields"][0]]}

    def test_include_jmespath_specific_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include
              file: {self.nested_fields_schema_file}
              jmespath: fields[?contains(['submission_timestamp', 'sample_id'], name)]
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][0],
                nested_fields_schema["fields"][3],
            ]
        }

    def test_include_jmespath_most_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include
              file: {self.nested_fields_schema_file}
              jmespath: fields[?!contains(['client_info', 'normalized_app_name'], name)]
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][0],
                nested_fields_schema["fields"][3],
            ]
        }

    def test_include_jmespath_field_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: submission_date
              type: DATE
              description: !include
                file: {self.nested_fields_schema_file}
                jmespath: fields[?name == 'submission_timestamp'] | [0].description
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "submission_date",
                    "type": "DATE",
                    "description": nested_fields_schema["fields"][0]["description"],
                }
            ]
        }

    def test_include_jmespath_nested_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_information
              type: RECORD
              fields: !include
                file: {self.nested_fields_schema_file}
                jmespath: fields[?name == 'client_info'] | [0].fields
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_information",
                    "type": "RECORD",
                    "fields": nested_fields_schema["fields"][1]["fields"],
                }
            ]
        }

    # !include-field tag tests...
    def test_include_field(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": [nested_fields_schema["fields"][0]]}

    def test_include_nested_field(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.client_id
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": [nested_fields_schema["fields"][1]["fields"][0]]}

    def test_include_further_nested_field(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.distribution.name
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [nested_fields_schema["fields"][1]["fields"][2]["fields"][0]]
        }

    def test_include_field_new_type(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: sample_id
              new_type: NUMERIC
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [{**nested_fields_schema["fields"][3], "type": "NUMERIC"}]
        }

    def test_include_field_new_mode(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.distribution
              new_mode: REPEATED
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    **nested_fields_schema["fields"][1]["fields"][2],
                    "mode": "REPEATED",
                }
            ]
        }

    def test_include_field_new_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.client_id
              new_description: Actually what this means is...
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    **nested_fields_schema["fields"][1]["fields"][0],
                    "description": "Actually what this means is...",
                }
            ]
        }

    def test_include_field_append_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.client_id
              append_description: And one more thing...
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    **nested_fields_schema["fields"][1]["fields"][0],
                    "description": (
                        nested_fields_schema["fields"][1]["fields"][0][
                            "description"
                        ].rstrip()
                        + "\nAnd one more thing..."
                    ),
                }
            ]
        }

    def test_include_field_prepend_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_table}
              field: client_info.client_id
              prepend_description: First let me say...
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    **nested_fields_schema["fields"][1]["fields"][0],
                    "description": (
                        "First let me say...\n"
                        + nested_fields_schema["fields"][1]["fields"][0][
                            "description"
                        ].lstrip()
                    ),
                }
            ]
        }

    def test_include_nonexistent_table_field(self):
        with pytest.raises(FileNotFoundError):
            schema_yaml = dedent("""
                fields:
                - !include-field
                  table: moz-fx-data-test-project.test.nonexistent
                  field: submission_timestamp
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_stable_table_field(
        self, nested_fields_schema, patch_get_stable_table_schemas
    ):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              table: {self.nested_fields_stable_table}
              field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": [nested_fields_schema["fields"][0]]}

    def test_include_nonexistent_stable_table_field(
        self, patch_get_stable_table_schemas
    ):
        with pytest.raises(Exception):
            schema_yaml = dedent("""
                fields:
                - !include-field
                  table: moz-fx-data-test-project.test_stable.nonexistent
                  field: submission_timestamp
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_file_field(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - !include-field
              file: {self.nested_fields_schema_file}
              field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": [nested_fields_schema["fields"][0]]}

    def test_include_nonexistent_field(self):
        with pytest.raises(KeyError):
            schema_yaml = dedent(f"""
                fields:
                - !include-field
                  file: {self.nested_fields_schema_file}
                  field: nonexistent
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_nonexistent_parent_field(self):
        with pytest.raises(KeyError):
            schema_yaml = dedent(f"""
                fields:
                - !include-field
                  file: {self.nested_fields_schema_file}
                  field: ping_info.client_id
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_nonexistent_nested_field(self):
        with pytest.raises(KeyError):
            schema_yaml = dedent(f"""
                fields:
                - !include-field
                  file: {self.nested_fields_schema_file}
                  field: client_info.ping_type
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    # !include-fields tag tests...
    def test_include_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_table}
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": nested_fields_schema["fields"]}

    def test_include_specific_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_table}
              field_names:
              - submission_timestamp
              - sample_id
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][0],
                nested_fields_schema["fields"][3],
            ]
        }

    def test_include_specific_fields_in_different_order(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_table}
              field_names:
              - sample_id
              - submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][3],
                nested_fields_schema["fields"][0],
            ]
        }

    def test_include_most_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_table}
              exclude_field_names:
              - client_info
              - normalized_app_name
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][0],
                nested_fields_schema["fields"][3],
            ]
        }

    def test_include_fields_with_replacements(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_table}
              field_replacements:
              - name: client_info
                type: JSON
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                nested_fields_schema["fields"][0],
                {"name": "client_info", "type": "JSON"},
                nested_fields_schema["fields"][2],
                nested_fields_schema["fields"][3],
            ]
        }

    def test_include_nested_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_information
              type: RECORD
              fields: !include-fields
                table: {self.nested_fields_table}
                parent_field: client_info
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_information",
                    "type": "RECORD",
                    "fields": nested_fields_schema["fields"][1]["fields"],
                }
            ]
        }

    def test_include_specific_nested_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_information
              type: RECORD
              fields: !include-fields
                table: {self.nested_fields_table}
                parent_field: client_info
                field_names:
                - client_id
                - app_channel
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_information",
                    "type": "RECORD",
                    "fields": [
                        nested_fields_schema["fields"][1]["fields"][0],
                        nested_fields_schema["fields"][1]["fields"][1],
                    ],
                }
            ]
        }

    def test_include_most_nested_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_information
              type: RECORD
              fields: !include-fields
                table: {self.nested_fields_table}
                parent_field: client_info
                exclude_field_names:
                - distribution
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_information",
                    "type": "RECORD",
                    "fields": [
                        nested_fields_schema["fields"][1]["fields"][0],
                        nested_fields_schema["fields"][1]["fields"][1],
                    ],
                }
            ]
        }

    def test_include_nested_fields_with_replacements(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_information
              type: RECORD
              fields: !include-fields
                table: {self.nested_fields_table}
                parent_field: client_info
                field_replacements:
                - name: distribution
                  type: JSON
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_information",
                    "type": "RECORD",
                    "fields": [
                        nested_fields_schema["fields"][1]["fields"][0],
                        nested_fields_schema["fields"][1]["fields"][1],
                        {"name": "distribution", "type": "JSON"},
                    ],
                }
            ]
        }

    def test_include_nonexistent_table_fields(self):
        with pytest.raises(FileNotFoundError):
            schema_yaml = dedent("""
                fields: !include-fields
                  table: moz-fx-data-test-project.test.nonexistent
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_stable_table_fields(
        self, nested_fields_schema, patch_get_stable_table_schemas
    ):
        schema_yaml = dedent(f"""
            fields: !include-fields
              table: {self.nested_fields_stable_table}
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": nested_fields_schema["fields"]}

    def test_include_nonexistent_stable_table_fields(
        self, patch_get_stable_table_schemas
    ):
        with pytest.raises(Exception):
            schema_yaml = dedent("""
                fields: !include-fields
                  table: moz-fx-data-test-project.test_stable.nonexistent
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_file_fields(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !include-fields
              file: {self.nested_fields_schema_file}
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {"fields": nested_fields_schema["fields"]}

    def test_include_nonexistent_fields(self):
        with pytest.raises(KeyError):
            schema_yaml = dedent(f"""
                fields:
                - !include-fields
                  file: {self.nested_fields_schema_file}
                  field_names:
                  - nonexistent
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    # !include-field-description tag tests...
    def test_include_field_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: submission_date
              type: DATE
              description: !include-field-description
                table: {self.nested_fields_table}
                field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "submission_date",
                    "type": "DATE",
                    "description": nested_fields_schema["fields"][0]["description"],
                }
            ]
        }

    def test_include_nested_field_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_id
              type: STRING
              description: !include-field-description
                table: {self.nested_fields_table}
                field: client_info.client_id
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_id",
                    "type": "STRING",
                    "description": nested_fields_schema["fields"][1]["fields"][0][
                        "description"
                    ],
                }
            ]
        }

    def test_include_field_description_append(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_id
              type: STRING
              description: !include-field-description
                table: {self.nested_fields_table}
                field: client_info.client_id
                append: And one more thing...
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_id",
                    "type": "STRING",
                    "description": (
                        nested_fields_schema["fields"][1]["fields"][0][
                            "description"
                        ].rstrip()
                        + "\nAnd one more thing..."
                    ),
                }
            ]
        }

    def test_include_field_description_prepend(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: client_id
              type: STRING
              description: !include-field-description
                table: {self.nested_fields_table}
                field: client_info.client_id
                prepend: First let me say...
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "client_id",
                    "type": "STRING",
                    "description": (
                        "First let me say...\n"
                        + nested_fields_schema["fields"][1]["fields"][0][
                            "description"
                        ].lstrip()
                    ),
                }
            ]
        }

    def test_include_nonexistent_table_field_description(self):
        with pytest.raises(FileNotFoundError):
            schema_yaml = dedent("""
                fields:
                - name: submission_date
                  type: DATE
                  description: !include-field-description
                    table: moz-fx-data-test-project.test.nonexistent
                    field: submission_timestamp
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_stable_table_field_description(
        self, nested_fields_schema, patch_get_stable_table_schemas
    ):
        schema_yaml = dedent(f"""
            fields:
            - name: submission_date
              type: DATE
              description: !include-field-description
                table: {self.nested_fields_stable_table}
                field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "submission_date",
                    "type": "DATE",
                    "description": nested_fields_schema["fields"][0]["description"],
                }
            ]
        }

    def test_include_nonexistent_stable_table_field_description(
        self, patch_get_stable_table_schemas
    ):
        with pytest.raises(Exception):
            schema_yaml = dedent("""
                fields:
                - name: submission_date
                  type: DATE
                  description: !include-field-description
                    table: moz-fx-data-test-project.test_stable.nonexistent
                    field: submission_timestamp
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    def test_include_file_field_description(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields:
            - name: submission_date
              type: DATE
              description: !include-field-description
                file: {self.nested_fields_schema_file}
                field: submission_timestamp
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {
                    "name": "submission_date",
                    "type": "DATE",
                    "description": nested_fields_schema["fields"][0]["description"],
                }
            ]
        }

    def test_include_nonexistent_field_description(self):
        with pytest.raises(KeyError):
            schema_yaml = dedent(f"""
                fields:
                - name: submission_date
                  type: DATE
                  description: !include-field-description
                    file: {self.nested_fields_schema_file}
                    field: nonexistent
            """)
            yaml.load(schema_yaml, Loader=PatchedSchemaLoader)

    # !flatten-lists tag tests...
    def test_flatten_lists(self, nested_fields_schema):
        schema_yaml = dedent(f"""
            fields: !flatten-lists
            - name: first_seen_date
              type: DATE
            - !include-fields
              table: {self.nested_fields_table}
              field_names:
              - submission_timestamp
              - sample_id
        """)
        result = yaml.load(schema_yaml, Loader=PatchedSchemaLoader)
        assert result == {
            "fields": [
                {"name": "first_seen_date", "type": "DATE"},
                nested_fields_schema["fields"][0],
                nested_fields_schema["fields"][3],
            ]
        }
