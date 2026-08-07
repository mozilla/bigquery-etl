import json
from types import SimpleNamespace

import httpx
import pytest
from google.genai import errors

from bigquery_etl.data_governance.classification import models as models_module
from bigquery_etl.data_governance.classification.config import (
    DEFAULT_MODEL,
    ClassificationConfig,
)
from bigquery_etl.data_governance.classification.models import (
    TokenTotals,
    VertexModel,
    _is_transient,
    _strip_json_fences,
    model_for,
)


def response(text, prompt_tokens=10, candidates_tokens=5, thoughts_tokens=0):
    """A stand-in for a genai GenerateContentResponse."""
    return SimpleNamespace(
        text=text,
        usage_metadata=SimpleNamespace(
            prompt_token_count=prompt_tokens,
            candidates_token_count=candidates_tokens,
            thoughts_token_count=thoughts_tokens,
        ),
    )


class FakeModels:
    """The `.models` namespace of a genai client, driven by a list of outcomes.

    Each outcome is either a response object (returned) or an exception instance
    (raised). The last outcome repeats once the list runs out.
    """

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def generate_content(self, model, contents):
        self.calls.append({"model": model, "contents": contents})
        outcome = self.outcomes[min(len(self.calls) - 1, len(self.outcomes) - 1)]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def fake_client(outcomes):
    """Stand in for genai.Client, whose only used surface is `.models`."""
    return SimpleNamespace(models=FakeModels(outcomes))


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record the delays time.sleep was asked for, so no test ever sleeps."""
    recorded = []
    monkeypatch.setattr(models_module.time, "sleep", recorded.append)
    return recorded


def build_model(outcomes, **kwargs):
    client = fake_client(outcomes)
    model = VertexModel(
        model=DEFAULT_MODEL,
        project="test-project",
        location="global",
        client=client,
        **kwargs,
    )
    return model, client.models


@pytest.mark.parametrize(
    "text",
    [
        '{"primary_label": "user.behavior"}',
        '```json\n{"primary_label": "user.behavior"}\n```',
        '```\n{"primary_label": "user.behavior"}\n```',
        '\n\n  ```json\n{"primary_label": "user.behavior"}\n```  \n',
    ],
    ids=["bare", "json_fence", "bare_fence", "surrounding_whitespace"],
)
def test_strip_json_fences(text):
    assert json.loads(_strip_json_fences(text)) == {"primary_label": "user.behavior"}


@pytest.mark.parametrize(
    "exc,expected",
    [
        (errors.ServerError(503, {"error": {"message": "unavailable"}}), True),
        (errors.ClientError(400, {"error": {"message": "invalid argument"}}), False),
        (errors.ClientError(429, {"error": {"message": "quota"}}), True),
        # A dropped connection carries no status code, so it is not retried.
        (httpx.ReadTimeout(""), False),
        (RuntimeError("502 Bad Gateway"), False),
    ],
    ids=["server_503", "client_400", "client_429", "read_timeout", "plain_error"],
)
def test_is_transient(exc, expected):
    assert _is_transient(exc) is expected


class TestGenerateJson:
    def test_parses_fenced_response_and_counts_tokens(self, sleeps):
        model, fake = build_model(
            [
                response(
                    '```json\n{"primary_label": "technical"}\n```',
                    prompt_tokens=120,
                    candidates_tokens=30,
                    thoughts_tokens=45,
                )
            ]
        )

        assert model.generate_json("prompt") == {"primary_label": "technical"}
        assert fake.calls == [{"model": DEFAULT_MODEL, "contents": "prompt"}]
        assert model.totals == TokenTotals(calls=1, input_tokens=120, output_tokens=75)
        assert sleeps == []

    def test_missing_usage_metadata_counts_zeros(self):
        model, _ = build_model([SimpleNamespace(text='{"primary_label": "technical"}')])

        assert model.generate_json("prompt") == {"primary_label": "technical"}
        assert model.totals == TokenTotals(calls=1, input_tokens=0, output_tokens=0)

    def test_totals_accumulate_across_calls(self):
        model, _ = build_model(
            [response('{"a": 1}', prompt_tokens=7, candidates_tokens=3)]
        )

        model.generate_json("one")
        model.generate_json("two")

        assert model.totals == TokenTotals(calls=2, input_tokens=14, output_tokens=6)

    def test_transient_failure_then_success(self, sleeps):
        model, fake = build_model(
            [
                errors.ServerError(503, {"error": {"message": "unavailable"}}),
                response('{"primary_label": "technical"}'),
            ],
            base_delay=1.5,
        )

        assert model.generate_json("prompt") == {"primary_label": "technical"}
        assert len(fake.calls) == 2
        assert sleeps == [1.5]
        # The 503 never produced a response, so there was nothing to bill.
        assert model.totals.calls == 1

    def test_non_transient_failure_raises_immediately(self, sleeps):
        model, fake = build_model(
            [errors.ClientError(400, {"error": {"message": "invalid argument"}})]
        )

        with pytest.raises(errors.ClientError):
            model.generate_json("prompt")

        assert len(fake.calls) == 1
        assert sleeps == []
        assert model.totals.calls == 0

    def test_retries_exhausted_raises_with_doubling_backoff(self, sleeps):
        last = errors.ServerError(502, {"error": {"message": "bad gateway"}})
        model, fake = build_model([last], retries=3, base_delay=2.0)

        with pytest.raises(errors.ServerError) as excinfo:
            model.generate_json("prompt")

        assert excinfo.value is last
        assert len(fake.calls) == 4
        assert sleeps == [2.0, 4.0, 8.0]

    def test_bad_json_is_not_retried_but_is_still_billed(self, sleeps):
        model, fake = build_model([response("not json at all")])

        with pytest.raises(json.JSONDecodeError):
            model.generate_json("prompt")

        assert len(fake.calls) == 1
        assert sleeps == []
        # The response came back and was billed; only its text was unusable.
        assert model.totals == TokenTotals(calls=1, input_tokens=10, output_tokens=5)


class TestModelFor:
    @pytest.fixture
    def make_client_spy(self, monkeypatch):
        """Record the project and location model_for passes to _make_client."""
        seen = {}

        def fake_make_client(project, location):
            seen["project"] = project
            seen["location"] = location
            return fake_client([response('{"a": 1}')])

        monkeypatch.setattr(models_module, "_make_client", fake_make_client)
        return seen

    def test_defaults_to_the_tables_project(self, make_client_spy):
        config = ClassificationConfig(run_id="r1", project="tables-project")

        model = model_for(config)

        assert make_client_spy == {"project": "tables-project", "location": "global"}
        assert model.model == config.model

    def test_vertex_project_overrides(self, make_client_spy):
        config = ClassificationConfig(
            run_id="r1",
            project="tables-project",
            model="gemini-3.5-pro",
            vertex_project="vertex-project",
            vertex_location="us-central1",
        )

        model = model_for(config)

        assert make_client_spy == {
            "project": "vertex-project",
            "location": "us-central1",
        }
        assert model.model == "gemini-3.5-pro"
