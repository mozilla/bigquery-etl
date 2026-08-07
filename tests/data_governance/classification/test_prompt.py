from bigquery_etl.data_governance.classification.prompt import (
    TOP_N_PROBES,
    _probes_block,
    _profile_block,
    build_classification_prompt,
    find_matching_probes,
    find_metric_probes,
)


def column(**overrides):
    """A column dict as the readers build it, overridable per test."""
    return {"column_name": "user_id", "data_type": "STRING", **overrides}


class TestProfileBlock:
    def test_full_stats_with_values(self):
        block = _profile_block(
            column(
                null_rate=1.5,
                distinct_count=42,
                values=[("a", 10), ("b", 3)],
                example_value="a",
            )
        )
        assert "Null rate: 1.5%" in block
        assert "Distinct values: 42" in block
        assert "Top values (value: frequency): 'a' (10), 'b' (3)" in block
        # values win over example_value; both are never shown.
        assert "Example value" not in block

    def test_example_value_when_values_empty(self):
        # The profiler sets example_value only for high-cardinality columns and
        # their samples are suppressed before rendering, so this pins the
        # renderer's contract rather than a path production reaches.
        block = _profile_block(
            column(null_rate=0.0, distinct_count=5, values=[], example_value="hello")
        )
        assert "Example value: hello" in block
        assert "Top values" not in block

    def test_high_cardinality_suppression_leaves_stats_only(self):
        # What suppress_high_cardinality_samples leaves behind.
        block = _profile_block(
            column(
                null_rate=0.2,
                distinct_count=990000,
                is_high_cardinality=True,
                values=[],
                example_value=None,
            )
        )
        assert block == ("Null rate: 0.2%\nDistinct values: 990000 (high cardinality)")

    def test_nothing_known(self):
        assert _profile_block(column()) == "No profiling stats available."

    def test_pii_suppressed_keeps_line_and_stats(self):
        block = _profile_block(
            column(column_tier="pii_suppressed", null_rate=0.0, distinct_count=17)
        )
        assert block.startswith("PII-suppressed: column name matched a known PII")
        assert "Treat as personal data." in block
        assert "Null rate: 0.0%" in block
        assert "Distinct values: 17" in block

    def test_glean_metric_short_circuits(self):
        block = _profile_block(
            column(is_glean_metric=True, null_rate=1.0, distinct_count=9)
        )
        assert block == (
            "Glean metric column - not data-profiled; classify from the metric"
            " definition and its data_sensitivity."
        )
        assert "Null rate" not in block

    def test_high_cardinality_marker(self):
        block = _profile_block(column(distinct_count=1000, is_high_cardinality=True))
        assert "Distinct values: 1000 (high cardinality)" in block
        assert "(high cardinality)" not in _profile_block(
            column(distinct_count=1000, is_high_cardinality=False)
        )

    def test_values_capped_at_ten_with_thousands_separator(self):
        values = [(f"v{i}", 1234 + i) for i in range(12)]
        block = _profile_block(column(values=values))
        # v9 present and v10 absent pins the cap at exactly 10.
        assert "'v9' (1,243)" in block
        assert "'v10'" not in block

    def test_zero_null_rate_and_distinct_count_still_render(self):
        block = _profile_block(column(null_rate=0, distinct_count=0))
        assert "Null rate: 0%" in block
        assert "Distinct values: 0" in block


class TestProbesBlock:
    def test_no_probes(self):
        assert _probes_block([]) == "Candidate probes: none matched."

    def test_probe_with_name_only(self):
        assert _probes_block([{"probe_name": "account.user_id"}]) == (
            "Candidate probes:\n  - name=account.user_id"
        )

    def test_field_order(self):
        block = _probes_block(
            [
                {
                    "probe_name": "account.user_id",
                    "probe_type": "string",
                    "data_sensitivity": ["highly_sensitive"],
                    "tags": ["accounts"],
                    "probe_description": "The user id.",
                }
            ]
        )
        assert block == (
            "Candidate probes:\n"
            "  - name=account.user_id | type=string |"
            " data_sensitivity=['highly_sensitive'] | tags=['accounts'] |"
            " desc=The user id."
        )

    def test_two_probes_on_their_own_lines(self):
        block = _probes_block(
            [{"probe_name": "a.b"}, {"probe_name": "c.d", "probe_type": "boolean"}]
        )
        assert block.splitlines() == [
            "Candidate probes:",
            "  - name=a.b",
            "  - name=c.d | type=boolean",
        ]


class TestBuildClassificationPrompt:
    def test_column_identity_and_taxonomy(self):
        prompt = build_classification_prompt(
            column(bq_description=None),
            table="accounts_db_external.users_v1",
            probes=[],
            taxonomy_json='[{"label":"user.account"}]',
        )
        assert "Table: accounts_db_external.users_v1" in prompt
        assert "Column: user_id" in prompt
        assert "Data type: STRING" in prompt
        assert '[{"label":"user.account"}]' in prompt
        assert "Existing column description" not in prompt
        assert '{"primary_label": "<label>", "secondary_labels": [], ' in prompt
        assert "no markdown fences" in prompt

    def test_embeds_the_profile_and_probe_blocks(self):
        # Without this the composition is untested: dropping either block from
        # the assembled text leaves every other assertion here passing.
        col = column(null_rate=0.5, distinct_count=7, values=[("a", 2)])
        probes = [{"probe_name": "account.user_id", "probe_type": "string"}]
        prompt = build_classification_prompt(
            col, table="d.t", probes=probes, taxonomy_json="[]"
        )
        assert f"Profiled data:\n{_profile_block(col)}\n\n" in prompt
        assert f"{_probes_block(probes)}\n\n" in prompt

    def test_keeps_the_tuned_label_selection_rule(self):
        # The wording is tuned against observed misclassifications, so a
        # rewording is a regression the assertions above would not catch.
        prompt = build_classification_prompt(
            column(), table="d.t", probes=[], taxonomy_json="[]"
        )
        assert (
            "Concretely, a column literally named `id` is the table's own row key"
            in prompt
        )

    def test_description_line_when_set(self):
        prompt = build_classification_prompt(
            column(bq_description="The account id."),
            table="d.t",
            probes=[],
            taxonomy_json="[]",
        )
        assert (
            "Existing column description (from BigQuery schema): The account id."
            in prompt
        )


class TestFindMatchingProbes:
    def test_scoring_order(self):
        exact = {"probe_name": "clientId"}
        prefix = {"probe_name": "client_id_of_the_sender"}
        substring = {"probe_name": "legacy_client_id_v2"}
        assert find_matching_probes("client_id", [substring, prefix, exact]) == [
            exact,
            prefix,
            substring,
        ]

    def test_capped_at_top_n(self):
        probes = [{"probe_name": f"client_id_{i}"} for i in range(TOP_N_PROBES + 2)]
        assert len(find_matching_probes("client_id", probes)) == TOP_N_PROBES

    def test_no_candidates(self):
        assert find_matching_probes("client_id", [{"probe_name": "os.version"}]) == []

    def test_probe_without_name_skipped(self):
        assert (
            find_matching_probes(
                "client_id", [{"probe_name": ""}, {"probe_type": "string"}]
            )
            == []
        )

    def test_ignores_case_and_underscores(self):
        probe = {"probe_name": "clientId"}
        assert find_matching_probes("client_id", [probe]) == [probe]


class TestFindMetricProbes:
    def test_exact_leaf_match(self):
        probe = {"probe_name": "account.user_id"}
        assert find_metric_probes("metrics.string.account_user_id", [probe]) == [probe]

    def test_sha256_leaf_does_not_take_the_shorter_probe(self):
        user_id = {"probe_name": "account.user_id"}
        sha = {"probe_name": "account.user_id_sha256"}
        assert find_metric_probes(
            "metrics.string.account_user_id_sha256", [user_id, sha]
        ) == [sha]

    def test_unmatched_leaf(self):
        assert (
            find_metric_probes(
                "metrics.string.account_something_else",
                [{"probe_name": "account.user_id"}],
            )
            == []
        )
