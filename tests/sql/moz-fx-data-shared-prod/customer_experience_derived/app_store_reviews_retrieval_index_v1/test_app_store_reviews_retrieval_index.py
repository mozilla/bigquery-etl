"""Test customer_experience_derived.app_store_reviews_retrieval_index_v1 script."""

import re
from pathlib import Path

import pytest

from bigquery_etl.util.common import render

_repo_root = Path(__file__).resolve().parents[5]
_query_dir = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/customer_experience_derived"
    / "app_store_reviews_retrieval_index_v1"
)
_index_table = (
    "`moz-fx-data-shared-prod.customer_experience_derived."
    "app_store_reviews_retrieval_index_v1`"
)

_LLM_FIELDS = [
    "review_summary_llm",
    "review_category_llm",
    "review_language_llm",
    "review_entities_llm",
    "review_topics_llm",
    "review_sentiment_score",
]

_UPDATED_COLUMNS = [
    "title",
    "body",
    *_LLM_FIELDS,
    "embedding",
    "metadata",
]


@pytest.fixture(scope="module")
def sql():
    """Render script.sql through Jinja, with whitespace collapsed to single spaces."""
    rendered = render("script.sql", template_folder=str(_query_dir), format=False)
    return re.sub(r"\s+", " ", rendered)


def _output_schema_fields(sql):
    """Return the field names declared in the AI.GENERATE output_schema."""
    schema = re.search(r"output_schema => '([^']*)'", sql)
    assert schema is not None, "no output_schema found in the rendered script"
    return [field.split()[0] for field in schema.group(1).split(", ")]


class TestRenderingAndOutputFields:
    """The script renders, and every field asked of the model is prompted and selected."""

    def test_every_declared_output_field_is_prompted_and_selected(self, sql):
        assert "{{" not in sql
        assert "{%" not in sql
        assert _output_schema_fields(sql) == _LLM_FIELDS
        prompt = sql.split("output_schema =>")[0]
        for field in _LLM_FIELDS:
            assert f"{field}:" in prompt
            assert f"llm_result.{field}" in sql


class TestLlmValueChecks:
    """A value outside what the prompt allows is stored as NULL or trimmed, and flagged."""

    def test_out_of_range_and_invalid_values_are_dropped_and_flagged(self, sql):
        assert (
            "IF( reviews_llm.llm_result.review_sentiment_score BETWEEN - 1 AND 1, "
            "reviews_llm.llm_result.review_sentiment_score, NULL ) "
            "AS review_sentiment_score" in sql
        )
        assert "['review_sentiment_score_out_of_range']" in sql
        assert "['review_category_llm_not_allowed']" in sql
        for field in ("review_entities_llm", "review_topics_llm"):
            assert f"ORDER BY pos LIMIT 3 ) AS {field}" in sql
            assert f"['{field}_over_limit']" in sql
            assert f"['{field}_has_empty_elements']" in sql


class TestNullValues:
    """Empty text and the text null in any case are stored as NULL or dropped from arrays."""

    def test_text_fields_store_null_for_empty_or_null_text(self, sql):
        for field, value in (
            ("review_summary_llm", "TRIM"),
            ("review_category_llm", "LOWER(TRIM"),
            ("review_language_llm", "TRIM"),
        ):
            closing = "))" if value == "LOWER(TRIM" else ")"
            assert (
                f"IF( IFNULL(LOWER(TRIM(reviews_llm.llm_result.{field})), '') "
                f"IN ('', 'null'), NULL, {value}(reviews_llm.llm_result.{field}{closing} "
                f") AS {field}" in sql
            )
            assert (
                f"IF( IFNULL(LOWER(TRIM(reviews_llm.llm_result.{field})), '') "
                f"IN ('', 'null'), ['{field}_missing']" in sql
            )

    def test_array_fields_drop_empty_or_null_text_elements(self, sql):
        for element in ("e", "t"):
            assert f"IFNULL(LOWER(TRIM({element})), '') NOT IN ('', 'null')" in sql


class TestFailureReasons:
    """The metadata records which enrichment step produced nothing."""

    def test_a_missing_embedding_and_each_missing_llm_field_are_tagged(self, sql):
        assert "embedding IS NOT NULL AS embedding_succeeded" in sql
        assert "IF(embedding IS NULL, ['embedding_missing'], [])" in sql
        for field in _LLM_FIELDS:
            assert f"['{field}_missing']" in sql


class TestWindowAndEnrichment:
    """Reviews of the last 7 days are read; only new or edited reviews reach the LLM."""

    def test_window_covers_seven_days_up_to_submission_date(self, sql):
        assert (
            "DATE(created_date) BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY) "
            "AND @submission_date" in sql
        )

    def test_llm_and_embedding_run_only_for_new_or_edited_reviews(self, sql):
        assert (
            "stored.id IS NULL "
            "OR stored.title IS DISTINCT FROM app_store_reviews.title "
            "OR stored.body IS DISTINCT FROM app_store_reviews.body" in sql
        )
        assert sql.count("AI.GENERATE(") == 1
        assert sql.count("AI.EMBED(") == 1
        llm = sql[sql.index("reviews_llm AS (") : sql.index("reviews_embedding AS (")]
        assert "FROM reviews_to_enrich )" in llm
        embedding = sql[sql.index("reviews_embedding AS (") : sql.index(") AS source")]
        assert "FROM reviews_to_enrich )" in embedding


class TestMerge:
    """One MERGE with no temp table; edited reviews are updated, new reviews inserted."""

    def test_single_merge_without_temp_table(self, sql):
        assert "CREATE TEMP TABLE" not in sql
        assert sql.count("MERGE INTO") == 1
        assert f"MERGE INTO {_index_table} AS target USING (" in sql
        assert ") AS source ON target.id = source.id" in sql

    def test_matched_rows_update_every_column_except_creation_date(self, sql):
        matched = sql[sql.index("WHEN MATCHED") : sql.index("WHEN NOT MATCHED")]
        assert "creation_date" not in matched
        assert (
            "UPDATE SET "
            + ", ".join(f"{column} = source.{column}" for column in _UPDATED_COLUMNS)
            in matched
        )

    def test_new_rows_insert_every_column(self, sql):
        inserted = sql[sql.index("WHEN NOT MATCHED") :]
        columns = ["creation_date", "id", *_UPDATED_COLUMNS]
        assert "INSERT ( " + ", ".join(columns) + " )" in inserted
        assert (
            "VALUES ( " + ", ".join(f"source.{column}" for column in columns) + " )"
            in inserted
        )
